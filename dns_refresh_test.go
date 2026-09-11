package sink

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

func TestDNSRefreshContinuesWithoutConnectionFailureAndStopsOnClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var refreshes, closes atomic.Int32
		underlying := manual.NewBuilderWithScheme("dns-refresh-test")
		underlying.ResolveNowCallback = func(resolver.ResolveNowOptions) { refreshes.Add(1) }
		underlying.CloseCallback = func() { closes.Add(1) }
		builder := &refreshingDNSBuilder{Builder: underlying, interval: 30 * time.Second}
		target := resolver.Target{}
		options := resolver.BuildOptions{}
		resolution, err := builder.Build(target, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(resolution.Close)
		time.Sleep(61 * time.Second)
		synctest.Wait()
		if got := refreshes.Load(); got != 2 {
			t.Fatalf("healthy connections need periodic refreshes, got %d", got)
		}
		refreshOptions := resolver.ResolveNowOptions{}
		resolution.ResolveNow(refreshOptions)
		if got := refreshes.Load(); got != 3 {
			t.Fatalf("connection-triggered refresh was not forwarded, got %d", got)
		}
		resolution.Close()
		time.Sleep(61 * time.Second)
		synctest.Wait()
		if refreshes.Load() != 3 || closes.Load() != 1 {
			t.Fatalf("closed resolver still refreshed or closed more than once: refreshes=%d closes=%d", refreshes.Load(), closes.Load())
		}
	})
}
