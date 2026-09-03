package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/pipeline"
)

func TestSymbolListSet(t *testing.T) {
	var l symbolList
	for _, v := range []string{"BTC-USDT", "eth-usdt", " SOL-USDT "} {
		if err := l.Set(v); err != nil {
			t.Fatalf("Set(%q): %v", v, err)
		}
	}
	want := symbolList{"BTC-USDT", "ETH-USDT", "SOL-USDT"}
	if l.String() != want.String() {
		t.Errorf("got %q, want %q", l.String(), want.String())
	}
	for _, v := range []string{"BTCUSDT", "-USDT", "BTC-", "BTC-USD-T", "BTC-USDT"} {
		if err := l.Set(v); err == nil {
			t.Errorf("Set(%q) was accepted", v)
		}
	}
}

// syncWriter lets the test read the log while run is still writing it.
type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// binanceFixture reads a fixture from the exchange package rather than
// copying it here. Those files are captured wire bytes whose provenance is
// documented in one place, and a copy would be a second file that nobody
// recaptures (see internal/exchange/binance/testdata/README.md).
func binanceFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "internal", "exchange", "binance", "testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return data
}

// capturedDeltas returns the captured depth payloads, one per line.
func capturedDeltas(t *testing.T) [][]byte {
	t.Helper()
	var out [][]byte
	for _, line := range bytes.Split(binanceFixture(t, "depth_stream.ndjson"), []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		t.Fatal("no captured deltas")
	}
	return out
}

// fakeVenue serves the REST endpoints ingestd needs and a websocket that
// replays the captured depth stream in the combined-stream envelope.
type fakeVenue struct {
	rest *httptest.Server
	ws   *httptest.Server
}

func newFakeVenue(t *testing.T) *fakeVenue {
	t.Helper()
	info := binanceFixture(t, "exchangeinfo.json")
	snapshot := binanceFixture(t, "depth_snapshot.json")
	deltas := capturedDeltas(t)

	v := &fakeVenue{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v3/exchangeInfo", func(w http.ResponseWriter, r *http.Request) {
		w.Write(info)
	})
	mux.HandleFunc("/api/v3/depth", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("symbol"); got != "BTCUSDT" {
			t.Errorf("depth request for %q, want BTCUSDT", got)
		}
		w.Write(snapshot)
	})
	v.rest = httptest.NewServer(mux)

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	v.ws = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/stream" {
			t.Errorf("websocket path = %q, want /stream", got)
		}
		streams := r.URL.Query().Get("streams")
		if !strings.Contains(streams, "btcusdt@aggTrade") || !strings.Contains(streams, "btcusdt@depth") {
			t.Errorf("streams = %q, want both the trade and depth streams", streams)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		// Replay the capture in a loop, wrapped as the combined endpoint
		// wraps it, until the client goes away.
		for i := 0; ; i++ {
			frame := fmt.Sprintf(`{"stream":"btcusdt@depth@100ms","data":%s}`, deltas[i%len(deltas)])
			if err := conn.WriteMessage(websocket.TextMessage, []byte(frame)); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}))
	t.Cleanup(func() {
		v.rest.Close()
		v.ws.Close()
	})
	return v
}

// TestRunEndToEnd starts the whole binary against a fake venue: instrument
// metadata over REST, the captured depth stream over a websocket in the
// combined envelope, snapshots over REST, and asserts that the books end up
// anchored and that the metrics endpoint says so.
func TestRunEndToEnd(t *testing.T) {
	venue := newFakeVenue(t)
	cfg := config{
		symbols:      symbolList{"BTC-USDT"},
		endpoint:     "ws" + strings.TrimPrefix(venue.ws.URL, "http"),
		restEndpoint: venue.rest.URL,
		shards:       2,
		metricsAddr:  "127.0.0.1:0",
		summaryEvery: 50 * time.Millisecond,
		// The captured snapshot is a fixed state while the stream keeps
		// running, so a comparison would diverge by construction. The
		// harness itself is covered in internal/pipeline.
		checkEvery: 0,
	}

	var out syncWriter
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, logger) }()

	addr := waitForMetricsAddr(t, &out)
	// Applied implies anchored: a delta only reaches a book once a snapshot
	// has given it a position to be applied from.
	body := waitForMetric(t, addr, "market_stream_deltas_applied_total", 1)

	for _, want := range []string{
		"market_stream_frames_total",
		"market_stream_events_total",
		"market_stream_deltas_applied_total",
		"market_stream_shard_queue_depth{shard=\"0\"}",
		"market_stream_subscriber_dropped_total{subscriber=\"stdout-log\"}",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %s:\n%s", want, body)
		}
	}
	if got := metricValue(t, body, "market_stream_decode_errors_total"); got != 0 {
		t.Errorf("decode errors = %d: the combined envelope is not being unwrapped", got)
	}
	if got := metricValue(t, body, "market_stream_snapshots_total"); got == 0 {
		t.Error("a delta was applied without a snapshot ever anchoring the book")
	}
	if got := metricValue(t, body, "market_stream_book_errors_total"); got != 0 {
		t.Errorf("book errors = %d", got)
	}

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz = HTTP %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("run = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancellation")
	}

	// The metrics listener must be gone with it.
	if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
		t.Error("the metrics server is still listening after shutdown")
	}
}

var metricsAddrRE = regexp.MustCompile(`"msg":"metrics listening","addr":"([^"]+)"`)

func waitForMetricsAddr(t *testing.T, out *syncWriter) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m := metricsAddrRE.FindStringSubmatch(out.String()); m != nil {
			return m[1]
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("the metrics server never reported an address. Log:\n%s", out.String())
	return ""
}

// waitForMetric polls /metrics until the named counter reaches want, and
// returns the body it last read.
func waitForMetric(t *testing.T, addr, name string, want uint64) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var body string
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body = string(raw)
		if metricValue(t, body, name) >= want {
			return body
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never reached %d. Last scrape:\n%s", name, want, body)
	return body
}

// metricValue reads a counter with no labels out of a Prometheus text body.
func metricValue(t *testing.T, body, name string) uint64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") {
			var v uint64
			if _, err := fmt.Sscanf(strings.TrimPrefix(line, name+" "), "%d", &v); err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	return 0
}

// stubSnapshotter satisfies pipeline.Snapshotter without a network.
type stubSnapshotter struct{}

func (stubSnapshotter) Snapshot(context.Context, model.Symbol) (model.Snapshot, error) {
	return model.Snapshot{}, errors.New("stub")
}

// TestMetricsTextFormat pins the shape of the exposition, since it is written
// by hand rather than by a client library: every metric carries a HELP and a
// TYPE, and every sample is a name and a value.
func TestMetricsTextFormat(t *testing.T) {
	router, err := pipeline.NewRouter(pipeline.RouterConfig{
		Shards: 2, QueueCap: 4, Snapshots: stubSnapshotter{},
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	pub := pipeline.NewPublisher()
	if err := pub.Subscribe(pipeline.NewLogSubscriber(`odd"name`, slog.New(slog.NewJSONHandler(io.Discard, nil)), nil), 1); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var buf bytes.Buffer
	writeMetrics(&buf, router, pub, &counters{})
	body := buf.String()

	var samples, types, helps int
	for _, line := range strings.Split(body, "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			helps++
		case strings.HasPrefix(line, "# TYPE "):
			types++
		case line != "":
			samples++
			if len(strings.Fields(line)) != 2 {
				t.Errorf("sample %q is not a name and a value", line)
			}
		}
	}
	if helps == 0 || helps != types {
		t.Errorf("%d HELP lines and %d TYPE lines, want an equal non-zero number", helps, types)
	}
	if samples < helps {
		t.Errorf("%d samples for %d metrics", samples, helps)
	}
	// Two shards means two gauge samples, which is the only place the
	// exposition repeats a metric name.
	if got := strings.Count(body, "market_stream_shard_queue_depth{"); got != 2 {
		t.Errorf("%d shard gauge samples, want 2", got)
	}
	if !strings.Contains(body, `market_stream_subscriber_dropped_total{subscriber="odd\"name"}`) {
		t.Errorf("a quote in a subscriber name was not escaped:\n%s", body)
	}
}
