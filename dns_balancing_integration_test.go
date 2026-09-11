//go:build integration

package sink_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This test keeps gRPC's real DNS resolver without changing any process-global
// settings. All traffic stays on loopback; no Kubernetes cluster is required.
func TestDialDiscoversDNSScaleChangesWithHealthyConnections(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
	}{
		{name: "default", interval: 0},
		{name: "one-second", interval: time.Second},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			opts := sink.DialOptions{
				TransportCredentials: insecure.NewCredentials(),
				DNSRefreshInterval:   test.interval,
			}
			testDNSScaleChanges(t, opts)
		})
	}
}

func testDNSScaleChanges(t *testing.T, opts sink.DialOptions) {
	t.Helper()
	ipv4, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ipv4.Close() })
	_, port, err := net.SplitHostPort(ipv4.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ipv6, err := net.Listen("tcp6", net.JoinHostPort("::1", port))
	if err != nil {
		t.Fatalf("DNS scaling integration test needs IPv6 loopback: %v", err)
	}
	t.Cleanup(func() { _ = ipv6.Close() })
	listeners := []net.Listener{ipv4, ipv6}
	backends := make([]*testSinkServer, len(listeners))
	servers := make([]*grpc.Server, len(listeners))
	connections := make([]*dnsCountingListener, len(listeners))
	for index, listener := range listeners {
		backend := &testSinkServer{}
		server := grpc.NewServer()
		sinkv1.RegisterSinkServer(server, backend)
		t.Cleanup(server.Stop)
		counted := &dnsCountingListener{Listener: listener}
		go func() { _ = server.Serve(counted) }()
		backends[index] = backend
		servers[index] = server
		connections[index] = counted
	}
	dnsAddress, dnsState := startScalingDNS(t)
	client, err := sink.Dial("dns://"+dnsAddress+"/"+net.JoinHostPort("sink.test", port), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	datasetOptions := sink.DatasetOptions{Store: "primary", Namespace: "test", Dataset: "dns-balance", Encoding: sink.DocumentEncodingJSON}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatal(err)
	}
	window := 40 * time.Second
	if opts.DNSRefreshInterval > 0 {
		window = 5*opts.DNSRefreshInterval + 2*time.Second
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*window+5*time.Second)
	defer cancel()
	attempts := 0
	write := func() {
		t.Helper()
		attempts++
		record := sink.Record{Key: sink.StringKey(fmt.Sprintf("record-%d", attempts)), Value: map[string]int{"value": attempts}}
		_, err := dataset.Upsert(ctx, sink.CompletionWaitUntilApplied, record)
		if err != nil {
			t.Fatalf("write during DNS scaling: %v", err)
		}
	}
	count := func(index int) int {
		backends[index].mu.Lock()
		defer backends[index].mu.Unlock()
		return backends[index].writeCalls
	}
	write()
	if count(0) != 1 || count(1) != 0 {
		t.Fatal("initial DNS answer did not select only the first backend")
	}
	dnsState.stage.Store(1)
	started := time.Now()
	deadline := started.Add(window)
	for count(1) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("new DNS backend received no traffic while the original connection stayed healthy")
		}
		write()
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("scale-out discovered in %s without closing the original connection", time.Since(started))
	beforeFirst, beforeSecond := count(0), count(1)
	for range 40 {
		write()
	}
	if count(0)-beforeFirst < 15 || count(1)-beforeSecond < 15 {
		t.Fatal("new DNS backend did not participate in round-robin balancing")
	}

	if opts.DNSRefreshInterval > 0 {
		dnsState.stage.Store(3)
		deadline = time.Now().Add(window)
		for dnsState.failedQueries.Load() == 0 {
			if time.Now().After(deadline) {
				t.Fatal("configured DNS refresh never queried the failing DNS server")
			}
			write()
			time.Sleep(20 * time.Millisecond)
		}
		for range 50 {
			write()
			time.Sleep(20 * time.Millisecond)
		}
		t.Log("writes continued while the DNS server returned SERVFAIL")
	}
	dnsState.stage.Store(2)
	deadline = time.Now().Add(window)
	stable := 0
	for stable < 30 {
		if time.Now().After(deadline) {
			t.Fatal("removed DNS backend continued receiving new requests")
		}
		before := count(0)
		write()
		if count(0) == before {
			stable++
		} else {
			stable = 0
		}
		time.Sleep(20 * time.Millisecond)
	}
	servers[0].GracefulStop()
	beforeFirst = count(0)
	for range 40 {
		write()
	}
	if count(0) != beforeFirst || count(0)+count(1) != attempts {
		t.Fatal("DNS scaling sent a request to the retired backend or replayed a mutation")
	}
	for index, listener := range connections {
		if got := listener.accepts.Load(); got != 1 {
			t.Fatalf("DNS refresh reconnected unchanged backend %d: %d connections", index, got)
		}
	}
	t.Logf("scale-in drained successfully; %d writes, zero errors or replays", attempts)
}

type dnsCountingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *dnsCountingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return connection, err
}

type scalingDNSState struct {
	stage         atomic.Int32
	failedQueries atomic.Int32
}

// Stage 0 serves IPv4 only, 1 serves both loopback backends, and 2 serves IPv6
// only. Stage 3 returns SERVFAIL. The long answer TTL verifies that scheduled
// refreshes query the designated server instead of reusing a local TTL cache.
func startScalingDNS(t *testing.T) (string, *scalingDNSState) {
	t.Helper()
	listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	state := &scalingDNSState{}
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	go func() {
		defer close(done)
		buffer := make([]byte, 4096)
		for {
			length, peer, err := listener.ReadFrom(buffer)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("read DNS query: %v", err)
				}
				return
			}
			query := dnsmessage.Message{}
			if err := query.Unpack(buffer[:length]); err != nil {
				t.Errorf("parse DNS query: %v", err)
				return
			}
			response := dnsmessage.Message{
				Header: dnsmessage.Header{
					ID: query.ID, Response: true, Authoritative: true,
					RecursionDesired: query.RecursionDesired, RecursionAvailable: true,
				},
				Questions: query.Questions,
			}
			stage := state.stage.Load()
			if stage == 3 {
				response.RCode = dnsmessage.RCodeServerFailure
				state.failedQueries.Add(1)
			}
			for _, question := range query.Questions {
				header := dnsmessage.ResourceHeader{Name: question.Name, Type: question.Type, Class: dnsmessage.ClassINET, TTL: 300}
				var body dnsmessage.ResourceBody
				switch {
				case question.Type == dnsmessage.TypeA && stage < 2:
					body = &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}
				case question.Type == dnsmessage.TypeAAAA && stage > 0 && stage < 3:
					body = &dnsmessage.AAAAResource{AAAA: [16]byte{15: 1}}
				}
				if body != nil {
					answer := dnsmessage.Resource{Header: header, Body: body}
					response.Answers = append(response.Answers, answer)
				}
			}
			packet, err := response.Pack()
			if err != nil {
				t.Errorf("encode DNS answer: %v", err)
				return
			}
			if _, err := listener.WriteTo(packet, peer); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("send DNS answer: %v", err)
				return
			}
		}
	}()
	return listener.LocalAddr().String(), state
}
