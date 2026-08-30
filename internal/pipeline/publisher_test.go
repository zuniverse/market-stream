package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.uber.org/goleak"

	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
)

// TestMain asserts that no test in this package leaves a goroutine behind.
// The Publisher owns one goroutine per subscriber, so a missed Close or a
// delivery loop with the wrong exit condition shows up here.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// tradeEvent returns an event tagged with id in its price, so that tests can
// identify which events survived.
func tradeEvent(id int64) model.Event {
	return model.Event{
		Kind: model.KindTrade,
		Trade: model.Trade{
			Symbol: "BTC-USDT",
			Price:  model.Price(id),
			Qty:    1,
		},
	}
}

// collector records every event it consumes. When gate is non-nil, Consume
// blocks on it, which lets a test hold the delivery goroutine still and fill
// the bounded channel deterministically.
type collector struct {
	name    string
	gate    chan struct{}
	entered chan struct{}

	mu  sync.Mutex
	got []model.Event
}

func newCollector(name string) *collector {
	return &collector{name: name}
}

func (c *collector) Name() string { return c.name }

func (c *collector) Consume(_ context.Context, ev model.Event) {
	c.mu.Lock()
	c.got = append(c.got, ev)
	c.mu.Unlock()

	if c.gate != nil {
		// Signal the first entry, then wait for the test to release.
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-c.gate
	}
}

func (c *collector) ids() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int64, len(c.got))
	for i, ev := range c.got {
		out[i] = int64(ev.Trade.Price)
	}
	return out
}

func TestPublisherFansOutToEverySubscriber(t *testing.T) {
	a, b := newCollector("a"), newCollector("b")

	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(a, 8); err != nil {
		t.Fatal(err)
	}
	if err := pub.Subscribe(b, 8); err != nil {
		t.Fatal(err)
	}
	if err := pub.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	for i := int64(1); i <= 4; i++ {
		pub.Publish(tradeEvent(i))
	}
	pub.Close() // drains before returning

	want := []int64{1, 2, 3, 4}
	for _, c := range []*collector{a, b} {
		if got := c.ids(); !equal(got, want) {
			t.Errorf("subscriber %q received %v, want %v", c.Name(), got, want)
		}
	}
	for name, n := range pub.Dropped() {
		if n != 0 {
			t.Errorf("subscriber %q dropped %d events, want 0", name, n)
		}
	}
}

// TestPublisherDropsOldestWhenFull is the core backpressure-policy test: a
// full subscriber channel loses its oldest queued event, not the newest, and
// the loss is counted.
func TestPublisherDropsOldestWhenFull(t *testing.T) {
	slow := newCollector("slow")
	slow.gate = make(chan struct{})
	slow.entered = make(chan struct{}, 1)

	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(slow, 2); err != nil {
		t.Fatal(err)
	}
	if err := pub.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Event 1 is picked up by the delivery goroutine, which then parks inside
	// Consume. Waiting for that leaves the channel empty and this goroutine as
	// the only party touching it, so the rest of the sequence is deterministic.
	pub.Publish(tradeEvent(1))
	<-slow.entered

	// Capacity is 2. Events 2 and 3 fill the queue; 4 evicts 2, and 5 evicts 3.
	for i := int64(2); i <= 5; i++ {
		pub.Publish(tradeEvent(i))
	}

	if got, want := pub.Dropped()["slow"], uint64(2); got != want {
		t.Errorf("dropped = %d, want %d", got, want)
	}

	close(slow.gate)
	pub.Close()

	// Event 1 was already in flight; 4 and 5 are the survivors of the queue.
	if got, want := slow.ids(), []int64{1, 4, 5}; !equal(got, want) {
		t.Errorf("consumed %v, want %v", got, want)
	}
}

func TestPublisherRejectsInvalidSubscriptions(t *testing.T) {
	pub := pipeline.NewPublisher()

	if err := pub.Subscribe(newCollector("zero"), 0); err == nil {
		t.Error("Subscribe with capacity 0 must fail: every channel needs an explicit bound")
	}
	if err := pub.Subscribe(newCollector("neg"), -1); err == nil {
		t.Error("Subscribe with negative capacity must fail")
	}
	if err := pub.Subscribe(newCollector("dup"), 1); err != nil {
		t.Fatal(err)
	}
	if err := pub.Subscribe(newCollector("dup"), 1); err == nil {
		t.Error("Subscribe with a duplicate name must fail")
	}

	if err := pub.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Subscribe(newCollector("late"), 1); !errors.Is(err, pipeline.ErrAlreadyStarted) {
		t.Errorf("Subscribe after Start error = %v, want ErrAlreadyStarted", err)
	}
	if err := pub.Start(context.Background()); !errors.Is(err, pipeline.ErrAlreadyStarted) {
		t.Errorf("second Start error = %v, want ErrAlreadyStarted", err)
	}
}

func TestPublisherCloseIsIdempotent(t *testing.T) {
	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(newCollector("a"), 1); err != nil {
		t.Fatal(err)
	}
	if err := pub.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	pub.Close()
	pub.Close() // must not panic on a second close of the same channels
}

// TestPublisherCloseWithoutStart covers the shutdown path when startup failed
// part way through and Close runs from a defer.
func TestPublisherCloseWithoutStart(t *testing.T) {
	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(newCollector("a"), 1); err != nil {
		t.Fatal(err)
	}
	pub.Close()
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
