package binance_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/zuniverse/market-stream/internal/exchange/binance"
	"github.com/zuniverse/market-stream/internal/model"
	"github.com/zuniverse/market-stream/internal/record"
)

// referencePath is the committed capture the whole baseline is measured
// against. Benchmarking hand-written payloads would measure a shape chosen to
// suit the decoder rather than the one the venue sends, which is the mistake
// D22 records from the other direction.
func referencePath() string {
	return filepath.Join("..", "..", "..", "docs", "baseline", "reference.msr.zst")
}

// referenceCorpus returns the recorded frames and the instrument metadata
// that goes with them.
func referenceCorpus(tb testing.TB) ([]model.Frame, *binance.InstrumentCache) {
	tb.Helper()
	f, err := os.Open(referencePath())
	if err != nil {
		tb.Skipf("reference recording unavailable: %v", err)
	}
	defer f.Close()

	r, err := record.NewReader(f)
	if err != nil {
		tb.Fatalf("read reference: %v", err)
	}
	defer r.Close()

	var (
		frames []model.Frame
		cache  *binance.InstrumentCache
	)
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatalf("read reference: %v", err)
		}
		switch rec.Kind {
		case record.KindMeta:
			if cache, err = binance.ParseExchangeInfo(rec.Payload); err != nil {
				tb.Fatalf("reference metadata: %v", err)
			}
		case record.KindFrame:
			frames = append(frames, rec.Frame())
		}
	}
	if cache == nil || len(frames) == 0 {
		tb.Fatalf("reference holds %d frames and metadata=%t", len(frames), cache != nil)
	}
	return frames, cache
}

// splitByKind separates the corpus into the two stream types, so that each
// can be measured on its own as well as in the mix the feed actually sends.
func splitByKind(tb testing.TB, frames []model.Frame, dec *binance.Decoder) (trades, deltas []model.Frame) {
	tb.Helper()
	for _, f := range frames {
		ev, err := dec.Decode(f)
		if err != nil {
			tb.Fatalf("reference frame does not decode: %v", err)
		}
		switch ev.Kind {
		case model.KindTrade:
			trades = append(trades, f)
		case model.KindBookDelta:
			deltas = append(deltas, f)
		}
	}
	return trades, deltas
}

// benchmarkDecode runs the decoder over a corpus, reporting per frame.
func benchmarkDecode(b *testing.B, dec *binance.Decoder, corpus []model.Frame) {
	if len(corpus) == 0 {
		b.Skip("no frames of this kind in the reference recording")
	}
	var bytes int64
	for _, f := range corpus {
		bytes += int64(len(f.Data))
	}
	b.SetBytes(bytes / int64(len(corpus)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		if _, err := dec.Decode(corpus[i%len(corpus)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeMixed is the decoder against the feed as it arrives, both
// stream types in the proportion the venue sent them. It is the number the
// baseline quotes, since it is the one the pipeline actually pays.
func BenchmarkDecodeMixed(b *testing.B) {
	frames, cache := referenceCorpus(b)
	benchmarkDecode(b, binance.NewDecoder(cache), frames)
}

func BenchmarkDecodeAggTrade(b *testing.B) {
	frames, cache := referenceCorpus(b)
	dec := binance.NewDecoder(cache)
	trades, _ := splitByKind(b, frames, dec)
	benchmarkDecode(b, dec, trades)
}

func BenchmarkDecodeDepthUpdate(b *testing.B) {
	frames, cache := referenceCorpus(b)
	dec := binance.NewDecoder(cache)
	_, deltas := splitByKind(b, frames, dec)
	benchmarkDecode(b, dec, deltas)
}

// BenchmarkParseDepth measures the snapshot parser. It runs once per resync
// rather than per frame, which is why D35 is willing to keep it out of the
// replay loop, and the figure is here to show the size of what was excluded.
func BenchmarkParseDepth(b *testing.B) {
	body := fixture(b, "depth_snapshot.json")
	cache, err := binance.ParseExchangeInfo(fixture(b, "exchangeinfo.json"))
	if err != nil {
		b.Fatal(err)
	}
	inst, ok := cache.LookupByNormalized("BTC-USDT")
	if !ok {
		b.Fatal("BTC-USDT missing from the metadata fixture")
	}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if _, err := binance.ParseDepth(body, inst, 20); err != nil {
			b.Fatal(err)
		}
	}
}
