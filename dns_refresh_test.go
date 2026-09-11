package sink

import (
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

type dnsTestClientConn struct {
	resolver.ClientConn
	updates atomic.Int32
	errors  atomic.Int32
}

func (cc *dnsTestClientConn) UpdateState(resolver.State) error {
	cc.updates.Add(1)
	return nil
}

func (cc *dnsTestClientConn) ReportError(error) {
	cc.errors.Add(1)
}

func TestDNSRefreshContinuesWithoutConnectionFailureAndStopsOnClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var builds, hints, closes atomic.Int32
		underlying := manual.NewBuilderWithScheme("dns-refresh-test")
		underlying.BuildCallback = func(resolver.Target, resolver.ClientConn, resolver.BuildOptions) { builds.Add(1) }
		underlying.ResolveNowCallback = func(resolver.ResolveNowOptions) { hints.Add(1) }
		underlying.CloseCallback = func() { closes.Add(1) }
		initial := resolver.State{}
		underlying.InitialState(initial)
		builder := &refreshingDNSBuilder{Builder: underlying, interval: 5 * time.Second}
		cc := &dnsTestClientConn{}
		target := resolver.Target{}
		options := resolver.BuildOptions{}
		resolution, err := builder.Build(target, cc, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(resolution.Close)
		time.Sleep(11 * time.Second)
		synctest.Wait()
		if builds.Load() != 3 || closes.Load() != 2 || cc.updates.Load() != 3 {
			t.Fatalf("expected independent scheduled lookups: builds=%d closes=%d updates=%d", builds.Load(), closes.Load(), cc.updates.Load())
		}
		refreshOptions := resolver.ResolveNowOptions{}
		resolution.ResolveNow(refreshOptions)
		synctest.Wait()
		if hints.Load() != 1 {
			t.Fatal("connection-triggered refresh was not forwarded")
		}
		resolution.Close()
		resolution.Close()
		time.Sleep(11 * time.Second)
		synctest.Wait()
		if builds.Load() != 3 || closes.Load() != 3 {
			t.Fatalf("closed resolver still refreshed or closed more than once: builds=%d closes=%d", builds.Load(), closes.Load())
		}
	})
}

func TestDNSRefreshDoesNotInterruptSlowOrFailedLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var builds, closes atomic.Int32
		underlying := manual.NewBuilderWithScheme("dns-refresh-test")
		underlying.BuildCallback = func(resolver.Target, resolver.ClientConn, resolver.BuildOptions) { builds.Add(1) }
		underlying.CloseCallback = func() { closes.Add(1) }
		builder := &refreshingDNSBuilder{Builder: underlying, interval: time.Second}
		cc := &dnsTestClientConn{}
		target := resolver.Target{}
		options := resolver.BuildOptions{}
		resolution, err := builder.Build(target, cc, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(resolution.Close)
		underlying.CC().ReportError(errors.New("temporary DNS failure"))
		time.Sleep(31 * time.Second)
		synctest.Wait()
		if builds.Load() != 1 || closes.Load() != 0 || cc.errors.Load() != 1 {
			t.Fatal("refresh interrupted the unresolved lookup or hid its error")
		}
		state := resolver.State{}
		underlying.UpdateState(state)
		time.Sleep(1500 * time.Millisecond)
		synctest.Wait()
		if builds.Load() != 2 || closes.Load() != 1 {
			t.Fatal("scheduled refresh did not resume after DNS recovery")
		}
	})
}

func TestDNSRefreshSkipsLiteralIP(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var builds atomic.Int32
		underlying := manual.NewBuilderWithScheme("dns")
		underlying.BuildCallback = func(resolver.Target, resolver.ClientConn, resolver.BuildOptions) { builds.Add(1) }
		initial := resolver.State{}
		underlying.InitialState(initial)
		builder := &refreshingDNSBuilder{Builder: underlying, interval: time.Second}
		cc := &dnsTestClientConn{}
		targetURL := url.URL{Scheme: "dns", Path: "/[::1]:8080"}
		target := resolver.Target{URL: targetURL}
		options := resolver.BuildOptions{}
		resolution, err := builder.Build(target, cc, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(resolution.Close)
		time.Sleep(time.Minute)
		synctest.Wait()
		if builds.Load() != 1 {
			t.Fatal("literal IP target acquired a DNS refresh timer")
		}
	})
}

func TestDialRejectsNegativeDNSRefreshInterval(t *testing.T) {
	options := DialOptions{DNSRefreshInterval: -time.Second}
	client, err := Dial("127.0.0.1:8080", options)
	if err == nil || client != nil {
		if client != nil {
			_ = client.Close()
		}
		t.Fatalf("invalid DNS interval accepted: client=%v err=%v", client, err)
	}
}
