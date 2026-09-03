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

// Frame is a raw websocket message received from Binance with its arrival
// time. Recording captures frames exactly as they arrive, before any
// decoding, so that the decoder stays inside the measured loop (D6).
type Frame struct {
	Data       []byte
	ReceivedAt time.Time
}

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
		delivered, err := t.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if delivered {
			// The connection lived long enough to carry data, so whatever
			// ended it is a new failure rather than a continuation of the
			// ones the backoff was counting. Without this, a socket that runs
			// for six hours and then drops resumes at the wait the last
			// outage climbed to, which is exactly the case where the venue is
			// known to be reachable and waiting a minute is pure lost data.
			wait = t.initialWait
		}
		if errors.Is(err, errServerShutdown) {
			wait = t.initialWait // planned restart; reconnect at once
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
// fails, ctx is cancelled, or a serverShutdown frame arrives. It reports
// whether the connection delivered at least one frame, which is the signal
// that it was healthy rather than merely accepted: a venue that is refusing
// service can still complete a handshake and close.
func (t *Transport) runOnce(ctx context.Context) (delivered bool, err error) {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, t.url, nil)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
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
				return delivered, ctx.Err()
			}
			return delivered, fmt.Errorf("read: %w", err)
		}
		if isServerShutdown(data) {
			return delivered, errServerShutdown
		}
		select {
		case t.out <- Frame{Data: data, ReceivedAt: time.Now()}:
			delivered = true
		case <-ctx.Done():
			return delivered, ctx.Err()
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
