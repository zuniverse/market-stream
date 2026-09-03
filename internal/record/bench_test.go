package record_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/zuniverse/market-stream/internal/record"
)

func referenceFile() string {
	return filepath.Join("..", "..", "docs", "baseline", "reference.msr.zst")
}

// BenchmarkReadReference measures one whole pass over the committed capture:
// zstd decompression, record framing, and the JSON decode of the snapshots.
//
// It is here to be subtracted. A replay reads the file twice, once to collect
// the metadata and the snapshots and once to replay the frames (D38), so this
// figure is the part of a replay profile that belongs to the harness rather
// than to the pipeline being measured.
func BenchmarkReadReference(b *testing.B) {
	path := referenceFile()
	info, err := os.Stat(path)
	if err != nil {
		b.Skipf("reference recording unavailable: %v", err)
	}
	b.SetBytes(info.Size())
	b.ReportAllocs()
	b.ResetTimer()

	var records int
	for range b.N {
		records = 0
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		r, err := record.NewReader(f)
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			records++
		}
		r.Close()
		f.Close()
	}
	b.ReportMetric(float64(records), "records/file")
}
