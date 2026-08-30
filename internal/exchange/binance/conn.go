package binance

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// Frame is a raw websocket message received from Binance with its arrival time.
type Frame struct {
	Data       []byte
	ReceivedAt time.Time
}

// Conn is a single Binance websocket connection.
// Run is the sole goroutine entry point; callers must not call it concurrently.
type Conn struct {
	url string
	out chan<- Frame
}

// NewConn returns a Conn that dials url and sends received frames to out.
// out must have a finite capacity; Conn never closes it.
func NewConn(url string, out chan<- Frame) *Conn {
	return &Conn{url: url, out: out}
}

// Run dials the websocket, reads frames, and forwards each to out until ctx is
// cancelled or the connection returns an error. Returns ctx.Err() on clean
// cancellation.
//
// Owner: the goroutine that calls Run. Exit: ctx cancelled or read error.
func (c *Conn) Run(ctx context.Context) error {
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("binance: dial %s: %w", c.url, err)
	}

	// Unblock ReadMessage when ctx is cancelled.
	// Owner: Conn.Run. Exit: ctx cancelled or done closed.
	done := make(chan struct{})
	defer close(done)
	go func() {
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
			return fmt.Errorf("binance: read: %w", err)
		}
		select {
		case c.out <- Frame{Data: data, ReceivedAt: time.Now()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
