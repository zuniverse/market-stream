package binance_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zuniverse/market-stream/internal/book"
	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
)

// btcusdt is the instrument the depth fixture belongs to: tickSize 0.01 gives
// two price decimals and stepSize 0.00001 gives five quantity decimals, as
// TestParseExchangeInfo asserts against the same exchangeInfo fixture.
var btcusdt = model.Instrument{Symbol: "BTC-USDT", PriceDecimals: 2, QtyDecimals: 5}

func TestRawSymbol(t *testing.T) {
	cache := loadCache(t)

	raw, inst, ok := cache.RawSymbol("BTC-USDT")
	if !ok {
		t.Fatal("RawSymbol(BTC-USDT): not found")
	}
	if raw != "BTCUSDT" {
		t.Errorf("raw = %q, want %q", raw, "BTCUSDT")
	}
	if inst != btcusdt {
		t.Errorf("inst = %+v, want %+v", inst, btcusdt)
	}

	if _, _, ok := cache.RawSymbol("BTCUSDT"); ok {
		t.Error("RawSymbol accepted a raw symbol where a normalised one belongs")
	}
	if _, _, ok := cache.RawSymbol("NOPE-USDT"); ok {
		t.Error("RawSymbol(NOPE-USDT) reported found")
	}
}

func TestParseDepth(t *testing.T) {
	snap, err := binance.ParseDepth(fixture(t, "depth_snapshot.json"), btcusdt, binance.MaxDepthLimit)
	if err != nil {
		t.Fatalf("ParseDepth: %v", err)
	}

	if snap.Symbol != "BTC-USDT" {
		t.Errorf("Symbol = %q, want BTC-USDT: the body carries none, so it comes from the instrument", snap.Symbol)
	}
	if snap.LastID != 99521168819 {
		t.Errorf("LastID = %d, want 99521168819", snap.LastID)
	}
	if len(snap.Bids) != 20 || len(snap.Asks) != 20 {
		t.Fatalf("got %d bids and %d asks, want 20 and 20", len(snap.Bids), len(snap.Asks))
	}

	// Fixed point at the instrument's exponents: 77798.66 at two price
	// decimals, 1.49168 at five quantity decimals. The wire pads both to
	// eight fractional digits regardless (D22).
	if want := (model.Level{Price: 7779866, Qty: 149168}); snap.Bids[0] != want {
		t.Errorf("Bids[0] = %+v, want %+v", snap.Bids[0], want)
	}
	if want := (model.Level{Price: 7779867, Qty: 45495}); snap.Asks[0] != want {
		t.Errorf("Asks[0] = %+v, want %+v", snap.Asks[0], want)
	}
	if want := (model.Level{Price: 7779523, Qty: 26}); snap.Bids[19] != want {
		t.Errorf("Bids[19] = %+v, want %+v", snap.Bids[19], want)
	}
	if snap.Truncated {
		t.Error("Truncated = true for a response far short of the limit")
	}
}

// TestSnapshotLoadsIntoBook is the M2.2 done criterion: a fetched snapshot
// loads into the M2.1 structure and its top of book is the exchange's.
func TestSnapshotLoadsIntoBook(t *testing.T) {
	snap, err := binance.ParseDepth(fixture(t, "depth_snapshot.json"), btcusdt, binance.MaxDepthLimit)
	if err != nil {
		t.Fatalf("ParseDepth: %v", err)
	}

	b := book.New(snap.Symbol)
	if err := b.Reset(snap.Bids, snap.Asks); err != nil {
		t.Fatalf("Reset from snapshot: %v", err)
	}

	if b.BidDepth() != 20 || b.AskDepth() != 20 {
		t.Errorf("depth = %d bids, %d asks, want 20 and 20", b.BidDepth(), b.AskDepth())
	}
	bestBid, ok := b.BestBid()
	if !ok || bestBid.Price != 7779866 || bestBid.Qty != 149168 {
		t.Errorf("BestBid = %+v, %v, want price 7779866 qty 149168", bestBid, ok)
	}
	bestAsk, ok := b.BestAsk()
	if !ok || bestAsk.Price != 7779867 || bestAsk.Qty != 45495 {
		t.Errorf("BestAsk = %+v, %v, want price 7779867 qty 45495", bestAsk, ok)
	}
	if b.Crossed() {
		t.Error("book loaded from a snapshot is crossed")
	}

	// Rendered back through the instrument's exponents, this is the pair a
	// reader can check against the exchange's own display by eye.
	if got := model.FormatFixed(int64(bestBid.Price), btcusdt.PriceDecimals); got != "77798.66" {
		t.Errorf("best bid renders as %q, want \"77798.66\"", got)
	}
	if got := model.FormatFixed(int64(bestAsk.Qty), btcusdt.QtyDecimals); got != "0.45495" {
		t.Errorf("best ask qty renders as %q, want \"0.45495\"", got)
	}

	// The snapshot is ordered on the wire, but the book must not depend on
	// that: reversing the input has to produce the same book.
	rev := book.New(snap.Symbol)
	if err := rev.Reset(reversed(snap.Bids), reversed(snap.Asks)); err != nil {
		t.Fatalf("Reset from reversed snapshot: %v", err)
	}
	if got, want := rev.Bids(9), b.Bids(9); !equalLevels(got, want) {
		t.Errorf("bids from reversed input = %v, want %v", got, want)
	}
}

func reversed(in []model.Level) []model.Level {
	out := make([]model.Level, len(in))
	for i, l := range in {
		out[len(in)-1-i] = l
	}
	return out
}

func equalLevels(a, b []model.Level) bool {
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

func TestParseDepthTruncation(t *testing.T) {
	data := fixture(t, "depth_snapshot.json")
	tests := []struct {
		name  string
		limit int
		want  bool
	}{
		{"limit above the levels returned", binance.MaxDepthLimit, false},
		{"limit exactly the levels returned", 20, true},
		{"limit below the levels returned", 10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap, err := binance.ParseDepth(data, btcusdt, tt.limit)
			if err != nil {
				t.Fatalf("ParseDepth: %v", err)
			}
			if snap.Truncated != tt.want {
				t.Errorf("Truncated = %v, want %v", snap.Truncated, tt.want)
			}
		})
	}
}

func TestParseDepthRejectsBadPayloads(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"not json", `nope`},
		{"missing lastUpdateId", `{"bids":[["1.00","1.00000"]],"asks":[]}`},
		{"zero lastUpdateId", `{"lastUpdateId":0,"bids":[],"asks":[]}`},
		{"negative lastUpdateId", `{"lastUpdateId":-1,"bids":[],"asks":[]}`},
		{"unparseable price", `{"lastUpdateId":7,"bids":[["oops","1.00000"]],"asks":[]}`},
		{"price finer than the tick size", `{"lastUpdateId":7,"bids":[],"asks":[["78737.269","1.00000"]]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := binance.ParseDepth([]byte(tt.body), btcusdt, binance.MaxDepthLimit); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestParseDepthEmptyBook(t *testing.T) {
	// A symbol with no resting orders is unusual but valid, and must not be
	// confused with a malformed response.
	snap, err := binance.ParseDepth([]byte(`{"lastUpdateId":42,"bids":[],"asks":[]}`), btcusdt, binance.MaxDepthLimit)
	if err != nil {
		t.Fatalf("ParseDepth: %v", err)
	}
	if snap.LastID != 42 || len(snap.Bids) != 0 || len(snap.Asks) != 0 || snap.Truncated {
		t.Errorf("got %+v, want an empty untruncated snapshot at id 42", snap)
	}
}

func TestNewDepthClientLimit(t *testing.T) {
	cache := loadCache(t)
	tests := []struct {
		in   int
		want int
	}{
		{100, 100},
		{binance.MaxDepthLimit, binance.MaxDepthLimit},
		{0, binance.MaxDepthLimit},
		{-1, binance.MaxDepthLimit},
		{binance.MaxDepthLimit + 1, binance.MaxDepthLimit},
	}
	for _, tt := range tests {
		if got := binance.NewDepthClient(http.DefaultClient, "http://x", cache, tt.in).Limit(); got != tt.want {
			t.Errorf("NewDepthClient(limit %d).Limit() = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestDepthClientSnapshot(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		w.Write(fixture(t, "depth_snapshot.json"))
	}))
	defer srv.Close()

	c := binance.NewDepthClient(srv.Client(), srv.URL, loadCache(t), 0)
	snap, err := c.Snapshot(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	if gotPath != "/api/v3/depth" {
		t.Errorf("path = %q, want /api/v3/depth", gotPath)
	}
	// The request must carry the exchange's own symbol form: the normalised
	// one would be rejected, and translating it is this package's job (D16).
	if got := gotQuery.Get("symbol"); got != "BTCUSDT" {
		t.Errorf("symbol param = %q, want BTCUSDT", got)
	}
	if got := gotQuery.Get("limit"); got != "5000" {
		t.Errorf("limit param = %q, want 5000", got)
	}
	if snap.Symbol != "BTC-USDT" || snap.LastID != 99521168819 || len(snap.Bids) != 20 {
		t.Errorf("snapshot = %+v", snap)
	}
}

func TestDepthClientUnknownSymbol(t *testing.T) {
	// An unknown symbol must fail before any request is made: the raw form
	// cannot be guessed, and guessing it would send a request that costs
	// weight and returns a Binance error anyway.
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	defer srv.Close()

	c := binance.NewDepthClient(srv.Client(), srv.URL, loadCache(t), 0)
	if _, err := c.Snapshot(context.Background(), "NOPE-USDT"); err == nil {
		t.Error("expected an error for an unknown symbol")
	}
	if called {
		t.Error("an unknown symbol reached the network")
	}
}

func TestDepthClientHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
	}))
	defer srv.Close()

	c := binance.NewDepthClient(srv.Client(), srv.URL, loadCache(t), 0)
	_, err := c.Snapshot(context.Background(), "BTC-USDT")
	if err == nil {
		t.Fatal("expected an error for HTTP 400")
	}
	// The exchange's own explanation is the useful half of the error.
	if !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "Invalid symbol.") {
		t.Errorf("error = %v, want it to carry the status and the exchange message", err)
	}
}

func TestDepthClientContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := binance.NewDepthClient(srv.Client(), srv.URL, loadCache(t), 0)
	_, err := c.Snapshot(ctx, "BTC-USDT")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Snapshot with a cancelled context = %v, want it to wrap context.Canceled", err)
	}
}

// TestDecodeCapturedDepthStream runs the captured delta frames through the
// decoder as raw wire bytes.
//
// It exists because of D22: the fixtures that let a broken decoder pass every
// test were hand-written to the shape the code expected, so they could only
// ever confirm that expectation. These bytes came off the socket, so the
// assertions below are about Binance's behaviour rather than about mine.
func TestDecodeCapturedDepthStream(t *testing.T) {
	dec := binance.NewDecoder(loadCache(t))
	frames := capturedFrames(t)
	if len(frames) != 13 {
		t.Fatalf("fixture holds %d frames, want 13", len(frames))
	}

	var prevLast int64
	for i, data := range frames {
		ev, err := dec.Decode(binance.Frame{Data: data, ReceivedAt: time.Now()})
		if err != nil {
			t.Fatalf("frame %d: Decode: %v", i, err)
		}
		if ev.Kind != model.KindBookDelta {
			t.Fatalf("frame %d: Kind = %d, want KindBookDelta", i, ev.Kind)
		}
		d := ev.BookDelta
		if d.Symbol != "BTC-USDT" {
			t.Errorf("frame %d: Symbol = %q", i, d.Symbol)
		}
		if d.FirstID > d.LastID {
			t.Errorf("frame %d: U %d is above u %d", i, d.FirstID, d.LastID)
		}
		// One frame carries a whole range of update ids, not one each. Any
		// sequencing built on a single incrementing counter would be wrong,
		// and this capture spans 6 to over a thousand ids per frame.
		if d.LastID == d.FirstID {
			t.Logf("frame %d covers a single update id, which is legal but rare", i)
		}
		// Contiguity on an uninterrupted stream: the next frame opens exactly
		// where the previous one closed. This held across all 102 frames of
		// the capture these 13 were taken from.
		if i > 0 && d.FirstID != prevLast+1 {
			t.Errorf("frame %d: U = %d, want %d to continue the previous frame", i, d.FirstID, prevLast+1)
		}
		prevLast = d.LastID
	}
}

// TestCapturedStreamStraddlesSnapshot pins the relationship between the
// captured snapshot and the captured deltas, which is the input M2.3
// classifies. The websocket was connected and buffering before the snapshot
// was fetched, so the two genuinely overlap in time.
func TestCapturedStreamStraddlesSnapshot(t *testing.T) {
	snap, err := binance.ParseDepth(fixture(t, "depth_snapshot.json"), btcusdt, 20)
	if err != nil {
		t.Fatalf("ParseDepth: %v", err)
	}
	if !snap.Truncated {
		t.Error("Truncated = false for a 20 level response fetched at limit 20")
	}

	dec := binance.NewDecoder(loadCache(t))
	var stale, first, after int
	for i, data := range capturedFrames(t) {
		ev, err := dec.Decode(binance.Frame{Data: data})
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		d := ev.BookDelta
		switch {
		case d.LastID <= snap.LastID:
			stale++
		case d.FirstID <= snap.LastID+1:
			first++
		default:
			after++
		}
	}
	// Four frames were already folded into the snapshot, one carries the
	// first update the snapshot does not have, and the rest follow it.
	if stale != 4 || first != 1 || after != 8 {
		t.Errorf("classification = %d stale, %d first, %d after; want 4, 1, 8", stale, first, after)
	}
}

// capturedFrames returns the raw payloads from the captured delta stream, one
// per line, exactly as they arrived on the socket.
func capturedFrames(t *testing.T) [][]byte {
	t.Helper()
	var out [][]byte
	for _, line := range bytes.Split(fixture(t, "depth_stream.ndjson"), []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			out = append(out, line)
		}
	}
	return out
}
