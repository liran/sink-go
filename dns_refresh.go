package sink

import (
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
)

// refreshingDNSBuilder retains gRPC's DNS lookup, retry and service-config
// behavior. Each scheduled lookup starts a fresh DNS resolver so a per-client
// interval below gRPC's global minimum resolution interval actually takes
// effect. The gRPC channel and its healthy backend connections stay intact.
type refreshingDNSBuilder struct {
	resolver.Builder
	interval time.Duration
}

func (b *refreshingDNSBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	host := target.Endpoint()
	if parsed, _, err := net.SplitHostPort(host); err == nil {
		host = parsed
	}
	if _, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		return b.Builder.Build(target, cc, opts)
	}
	refreshing := &refreshingDNSResolver{
		ClientConn: cc,
		builder:    b,
		target:     target,
		options:    opts,
		updates:    make(chan struct{}, 1),
		hints:      make(chan struct{}, 1),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	underlying, err := b.Builder.Build(target, refreshing, opts)
	if err != nil {
		return nil, err
	}
	go refreshing.refresh(underlying)
	return refreshing, nil
}

type refreshingDNSResolver struct {
	resolver.ClientConn
	builder *refreshingDNSBuilder
	target  resolver.Target
	options resolver.BuildOptions
	updates chan struct{}
	hints   chan struct{}
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func (r *refreshingDNSResolver) UpdateState(state resolver.State) error {
	err := r.ClientConn.UpdateState(state)
	if err == nil {
		select {
		case r.updates <- struct{}{}:
		default:
		}
	}
	return err
}

func (r *refreshingDNSResolver) ResolveNow(resolver.ResolveNowOptions) {
	select {
	case r.hints <- struct{}{}:
	default:
	}
}

func (r *refreshingDNSResolver) refresh(underlying resolver.Resolver) {
	defer close(r.done)
	defer func() {
		if underlying != nil {
			underlying.Close()
		}
	}()
	timer := time.NewTimer(r.builder.interval)
	timer.Stop()
	defer timer.Stop()
	options := resolver.ResolveNowOptions{}
	for {
		select {
		case <-r.stop:
			return
		case <-r.updates:
			// Failed or slow lookups keep their original resolver and backoff.
			// Only an accepted update arms the next scheduled lookup.
			timer.Reset(r.builder.interval)
		case <-r.hints:
			if underlying != nil {
				underlying.ResolveNow(options)
			}
		case <-timer.C:
			if underlying != nil {
				underlying.Close()
			}
			// Close waits for old callbacks to finish. Discard any update that
			// raced with the timer so it cannot interrupt a new slow lookup.
			select {
			case <-r.updates:
			default:
			}
			var err error
			underlying, err = r.builder.Builder.Build(r.target, r, r.options)
			if err != nil {
				r.ReportError(err)
				timer.Reset(r.builder.interval)
			}
		}
	}
}

func (r *refreshingDNSResolver) Close() {
	r.once.Do(func() {
		close(r.stop)
		<-r.done
	})
}
