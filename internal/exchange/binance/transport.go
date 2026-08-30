package binance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/gorilla/websocket"
)

// Transport maintains a persistent Binance websocket connection.
// It reconnects with exponential backoff and jitter after any failure.
// A serverShutdown frame triggers an immediate reconnect without a delay.
type Transport struct {
	url         string
	out         chan<- Frame
	initialWait time.Duration
	maxWait     time.Duration
}

// Option configures a Transport.
type Option func(*Transport)

// WithInitialWait sets the starting reconnect backoff (default 500ms).
func WithInitialWait(d time.Duration) Option {
	return func(t *Transport) { t.initialWait = d }
}

// WithMaxWait caps the reconnect backoff (default 60s).
func WithMaxWait(d time.Duration) Option {
	return func(t *Transport) { t.maxWait = d }
}

// NewTransport returns a Transport that dials url and sends received frames
// to out. out must have a finite capacity; Transport never closes it.
func NewTransport(url string, out chan<- Frame, opts ...Option) *Transport {
	t := &Transport{
		url:         url,
		out:         out,
		initialWait: 500 * time.Millisecond,
		maxWait:     60 * time.Second,
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Run connects and reconnects until ctx is cancelled. Connection failures are
// followed by an exponential backoff with jitter. A serverShutdown frame is
// not forwarded and triggers an immediate reconnect. Returns ctx.Err().
//
// Owner: the goroutine that calls Run. Exit: ctx cancelled.
func (t *Transport) Run(ctx context.Context) error {
	wait := t.initialWait
	for {
		err := t.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errServerShutdown) {
			wait = t.initialWait // planned restart; reset backoff
			continue
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
		wait = nextWait(wait, t.maxWait)
	}
}

// runOnce establishes one connection and reads frames until the connection
// fails, ctx is cancelled, or a serverShutdown frame arrives.
func (t *Transport) runOnce(ctx context.Context) error {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, t.url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		// Owner: Transport.runOnce. Exit: ctx cancelled or done closed.
		select {
		case <-ctx.Done():
			ws.Close()
		case <-done:
		}
	}()
	defer ws.Close()

	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read: %w", err)
		}
		if isServerShutdown(data) {
			return errServerShutdown
		}
		select {
		case t.out <- Frame{Data: data, ReceivedAt: time.Now()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

var errServerShutdown = errors.New("serverShutdown")

func isServerShutdown(data []byte) bool {
	return bytes.Contains(data, []byte(`"serverShutdown"`))
}

// nextWait doubles current, caps at max, and adds up to 25% jitter.
func nextWait(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		next = max
	}
	if r := int64(next / 4); r > 0 {
		next += time.Duration(rand.Int64N(r))
	}
	return next
}
