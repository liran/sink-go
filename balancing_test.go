package sink_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	sink "github.com/liran/sink-go"
	sinkv1 "github.com/liran/sink-go/api/sink/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

func TestDialBalancesWritesAndFollowsEndpointChanges(t *testing.T) {
	backends := make([]*testSinkServer, 3)
	servers := make([]*grpc.Server, 3)
	addresses := make([]resolver.Address, 3)
	for index := range backends {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		backend := &testSinkServer{}
		server := grpc.NewServer()
		sinkv1.RegisterSinkServer(server, backend)
		t.Cleanup(server.Stop)
		go func() { _ = server.Serve(listener) }()
		backends[index] = backend
		servers[index] = server
		addresses[index].Addr = listener.Addr().String()
	}
	resolution := manual.NewBuilderWithScheme("sink-balancing-test")
	initial := resolver.State{Addresses: addresses[:2]}
	resolution.InitialState(initial)
	resolverOption := grpc.WithResolvers(resolution)
	opts := sink.DialOptions{TransportCredentials: insecure.NewCredentials(), GRPCOptions: []grpc.DialOption{resolverOption}}
	client, err := sink.Dial("sink-balancing-test:///sink", opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	datasetOptions := sink.DatasetOptions{Store: "primary", Namespace: "test", Dataset: "balance", Encoding: sink.DocumentEncodingJSON}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	attempts := 0
	write := func() {
		t.Helper()
		attempts++
		record := sink.Record{Key: sink.StringKey(fmt.Sprintf("record-%d", attempts)), Value: map[string]int{"value": attempts}}
		_, err := dataset.Upsert(ctx, sink.CompletionWaitUntilApplied, record)
		if err != nil {
			t.Fatalf("write through resolved backends: %v", err)
		}
	}
	count := func(index int) int {
		backends[index].mu.Lock()
		defer backends[index].mu.Unlock()
		return backends[index].writeCalls
	}
	for count(0) == 0 || count(1) == 0 {
		write()
	}
	beforeFirst, beforeSecond := count(0), count(1)
	for range 40 {
		write()
	}
	if count(0)-beforeFirst < 15 || count(1)-beforeSecond < 15 {
		t.Fatalf("writes are not balanced: first=%d second=%d", count(0)-beforeFirst, count(1)-beforeSecond)
	}
	updated := resolver.State{Addresses: addresses[1:]}
	resolution.UpdateState(updated)
	// Keep the retiring backend alive while the resolver update is applied,
	// as Kubernetes endpoint removal precedes the server's graceful drain.
	for count(2) == 0 {
		write()
	}
	servers[0].GracefulStop()
	beforeFirst, beforeSecond, beforeThird := count(0), count(1), count(2)
	for range 40 {
		write()
	}
	if count(0) != beforeFirst || count(1) == beforeSecond || count(2) == beforeThird {
		t.Fatal("client did not move writes to the updated backend set")
	}
	if count(0)+count(1)+count(2) != attempts {
		t.Fatal("load balancing replayed a mutation")
	}
}
