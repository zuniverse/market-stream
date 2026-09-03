package model_test

import (
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
)

// The strings are the wire format: Binance pads every decimal to eight
// fractional digits whatever the instrument's tickSize, so a price with two
// significant decimals still arrives with eight (D22). Benchmarking the
// trimmed form would measure a case that never occurs.
const (
	wirePrice = "78737.26000000"
	wireQty   = "0.01234000"
	wireZero  = "0.00000000"
)

func BenchmarkParsePrice(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		if _, err := model.ParsePrice(wirePrice, 2); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseQty(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		if _, err := model.ParseQty(wireQty, 5); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseQtyZero covers the deletion marker, which is a large share of
// every depth stream: a level going away is "0.00000000".
func BenchmarkParseQtyZero(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		if _, err := model.ParseQty(wireZero, 5); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFormatFixed(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_ = model.FormatFixed(7873726, 2)
	}
}
