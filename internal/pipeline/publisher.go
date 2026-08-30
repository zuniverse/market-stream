package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zuniverse/market-stream/internal/model"
)

// ErrAlreadyStarted is returned by Subscribe and Start once the Publisher is
// running. The subscriber set is fixed at startup so that Publish can read it
// without synchronisation on the hot path.
var ErrAlreadyStarted = errors.New("pipeline: publisher already started")

// Subscriber consumes events from the lossy side of the pipeline.
type Subscriber interface {
	// Name identifies the subscriber in logs and metrics. It must be unique
	// among the subscribers registered with one Publisher.
	Name() string

	// Consume handles one event. It is called from the subscriber's own
	// delivery goroutine, so implementations need no internal locking and see
	// events in publication order.
	//
	// A slow Consume costs its own subscriber dropped events and never slows
	// the pipeline. It must still return promptly: Close waits for the
	// in-flight call. ctx is cancelled on shutdown and is provided so that a
	// Consume doing real work can abandon it.
	Consume(ctx context.Context, ev model.Event)
}

// subscription is one registered subscriber and its bounded delivery channel.
type subscription struct {
	sub     Subscriber
	ch      chan model.Event
	dropped atomic.Uint64
}

// Publisher fans one event stream out to registered subscribers, each over its
// own bounded channel.
//
// This is the boundary between the lossless path (transport to book), where
// dropping a message corrupts state, and the lossy path (book to subscribers),
// where freshness matters more than completeness. When a subscriber channel is
// full, the oldest queued event is discarded to make room for the newest and a
// per-subscriber counter is incremented. Publish never blocks, so a slow
// subscriber cannot apply backpressure upstream.
//
// The lifecycle is Subscribe* -> Start -> Publish* -> Close. Publish and Close
// must be called by a single goroutine, the one owning the upstream stage.
type Publisher struct {
	// mu guards subs and started during setup only. Publish does not take it:
	// subs is never mutated after Start, and Subscribe fails once started.
	mu      sync.Mutex
	subs    []*subscription
	started bool

	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewPublisher returns a Publisher with no subscribers registered.
func NewPublisher() *Publisher {
	return &Publisher{}
}

// Subscribe registers s with a delivery channel of the given capacity, which
// must be positive: an unbounded channel has no overflow policy, and every
// channel in this pipeline has an explicit one. Subscribe must be called
// before Start.
func (p *Publisher) Subscribe(s Subscriber, capacity int) error {
	name := s.Name()
	if capacity <= 0 {
		return fmt.Errorf("pipeline: subscribe %q: capacity must be positive, got %d", name, capacity)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return fmt.Errorf("pipeline: subscribe %q: %w", name, ErrAlreadyStarted)
	}
	for _, existing := range p.subs {
		if existing.sub.Name() == name {
			return fmt.Errorf("pipeline: subscribe %q: name already registered", name)
		}
	}

	p.subs = append(p.subs, &subscription{
		sub: s,
		ch:  make(chan model.Event, capacity),
	})
	return nil
}

// Start launches one delivery goroutine per registered subscriber. After Start
// returns, Publish and Close may be called and Subscribe may not.
//
// Owner of each delivery goroutine: the Publisher. Exit: its channel is closed
// by Close, which happens exactly once.
func (p *Publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return ErrAlreadyStarted
	}
	p.started = true

	for _, s := range p.subs {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for ev := range s.ch {
				s.sub.Consume(ctx, ev)
			}
		}()
	}
	return nil
}

// Publish delivers ev to every subscriber and never blocks. When a subscriber
// channel is full, the oldest queued event is discarded and that subscriber's
// dropped counter is incremented.
//
// Publish is valid only between Start and Close, from a single goroutine.
func (p *Publisher) Publish(ev model.Event) {
	for _, s := range p.subs {
		select {
		case s.ch <- ev:
		default:
			// Full. Discard the oldest queued event to make room for the
			// newest. The retry cannot block in practice because Publish is
			// the only sender, but it stays non-blocking so that the "never
			// applies backpressure" property does not depend on that.
			select {
			case <-s.ch:
				s.dropped.Add(1)
			default:
			}
			select {
			case s.ch <- ev:
			default:
				s.dropped.Add(1)
			}
		}
	}
}

// Close stops delivery and waits for every subscriber to drain its queue and
// return. Draining is bounded work because every channel is bounded, so a
// clean shutdown does not discard the tail of the stream.
//
// Close must be called by the goroutine that calls Publish, and Publish must
// not be called afterwards. Repeated calls after the first do nothing.
func (p *Publisher) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		started := p.started
		p.mu.Unlock()
		if !started {
			return
		}
		for _, s := range p.subs {
			close(s.ch)
		}
		p.wg.Wait()
	})
}

// Dropped returns the number of events discarded so far, keyed by subscriber
// name. It is safe to call from any goroutine at any point in the lifecycle.
// M6 exposes these values as Prometheus counters.
func (p *Publisher) Dropped() map[string]uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]uint64, len(p.subs))
	for _, s := range p.subs {
		out[s.sub.Name()] = s.dropped.Load()
	}
	return out
}
