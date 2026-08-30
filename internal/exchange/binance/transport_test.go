package binance_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zuniverse/market-stream/internal/exchange/binance"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// wsURL converts an httptest server URL from http:// to ws://.
func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// TestTransportReconnectsOnDrop verifies that Transport reconnects after
// the server closes the connection.
func TestTransportReconnectsOnDrop(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"aggTrade","s":"BTCUSDT"}`))
		// Return immediately to drop the connection.
	}))
	defer srv.Close()

	out := make(chan binance.Frame, 4)
	tr := binance.NewTransport(wsURL(srv), out,
		binance.WithInitialWait(time.Millisecond),
		binance.WithMaxWait(10*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- tr.Run(ctx) }()

	// Drain two frames, proving two separate connections were made.
	<-out
	<-out

	if got := int(attempts.Load()); got < 2 {
		t.Errorf("expected >= 2 connection attempts, got %d", got)
	}

	cancel()
	select {
	case <-errc:
	case <-time.After(2 * time.Second):
		t.Error("Transport.Run did not return after context cancellation")
	}
}

// TestTransportServerShutdown verifies that a serverShutdown frame is not
// forwarded and triggers an immediate reconnect.
func TestTransportServerShutdown(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if n == 1 {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"serverShutdown"}`))
		} else {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"aggTrade","s":"BTCUSDT"}`))
		}
	}))
	defer srv.Close()

	out := make(chan binance.Frame, 4)
	tr := binance.NewTransport(wsURL(srv), out,
		binance.WithInitialWait(time.Millisecond),
		binance.WithMaxWait(10*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errc := make(chan error, 1)
	go func() { errc <- tr.Run(ctx) }()

	// The first frame on out must come from the second connection, not the shutdown.
	f := <-out
	if bytes.Contains(f.Data, []byte("serverShutdown")) {
		t.Error("serverShutdown frame must not be forwarded to the out channel")
	}
	if got := int(attempts.Load()); got < 2 {
		t.Errorf("expected >= 2 connections after serverShutdown, got %d", got)
	}

	cancel()
	select {
	case <-errc:
	case <-time.After(2 * time.Second):
		t.Error("Transport.Run did not return after context cancellation")
	}
}
