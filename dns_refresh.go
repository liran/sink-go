package sink

import (
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
)

// refreshingDNSBuilder retains gRPC's DNS lookup, retry and service-config
// behavior while discovering added backends even when every connection stays
// healthy. The built-in DNS resolver otherwise waits for ResolveNow after a
// successful lookup; its minimum resolution interval is not a polling timer.
type refreshingDNSBuilder struct {
	resolver.Builder
	interval time.Duration
}

func (b *refreshingDNSBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	underlying, err := b.Builder.Build(target, cc, opts)
	if err != nil {
		return nil, err
	}
	refreshing := &refreshingDNSResolver{
		Resolver: underlying,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go refreshing.refresh(b.interval)
	return refreshing, nil
}

type refreshingDNSResolver struct {
	resolver.Resolver
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func (r *refreshingDNSResolver) refresh(interval time.Duration) {
	defer close(r.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	options := resolver.ResolveNowOptions{}
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.Resolver.ResolveNow(options)
		}
	}
}

func (r *refreshingDNSResolver) Close() {
	r.once.Do(func() {
		close(r.stop)
		<-r.done
		r.Resolver.Close()
	})
}
