package binance

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/zuniverse/market-stream/internal/record"
)

// These tests are in package binance rather than binance_test: the scanner is
// unexported, and it is the part of the decoder that could be wrong in a way
// the round trip through encoding/json would hide.

// referenceCorpusInternal returns the raw frame payloads from the committed
// capture. It duplicates the loader in bench_test.go because that one lives
// in the external test package and this file has to reach unexported
// functions.
func referenceCorpusInternal(tb testing.TB) ([][]byte, int) {
	tb.Helper()
	f, err := os.Open(filepath.Join("..", "..", "..", "docs", "baseline", "reference.msr.zst"))
	if err != nil {
		tb.Skipf("reference recording unavailable: %v", err)
	}
	defer f.Close()

	r, err := record.NewReader(f)
	if err != nil {
		tb.Fatalf("read reference: %v", err)
	}
	defer r.Close()

	var frames [][]byte
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatalf("read reference: %v", err)
		}
		if rec.Kind == record.KindFrame {
			frames = append(frames, rec.Payload)
		}
	}
	return frames, len(frames)
}

func TestFieldValue(t *testing.T) {
	tests := []struct {
		name string
		data string
		key  string
		want string
		ok   bool
	}{
		{"first key", `{"a":1,"b":2}`, "a", "1", true},
		{"middle key", `{"a":1,"b":2,"c":3}`, "b", "2", true},
		{"last key", `{"a":1,"b":2}`, "b", "2", true},
		{"absent key", `{"a":1}`, "z", "", false},
		{"string value", `{"e":"depthUpdate"}`, "e", `"depthUpdate"`, true},
		{"object value", `{"data":{"e":"x","b":[1]}}`, "data", `{"e":"x","b":[1]}`, true},
		{"array value", `{"b":[["1.0","2.0"],["3.0","4.0"]]}`, "b", `[["1.0","2.0"],["3.0","4.0"]]`, true},
		{"nested braces in a string", `{"a":{"k":"}}}"},"b":2}`, "b", "2", true},
		{"escaped quote in a value", `{"a":"x\"y","b":2}`, "b", "2", true},
		{"escaped quote in a key", `{"a\"b":1,"c":2}`, "c", "2", true},
		{"key that is a prefix", `{"event":1,"e":2}`, "e", "2", true},
		{"case is exact", `{"E":1}`, "e", "", false},
		{"whitespace everywhere", "{ \"a\" : 1 , \"b\" : 2 }", "b", "2", true},
		{"negative number", `{"a":-1.5e3,"b":2}`, "a", "-1.5e3", true},
		{"literal null", `{"a":null,"b":2}`, "a", "null", true},
		{"empty object", `{}`, "a", "", false},
		{"not an object", `[1,2]`, "a", "", false},
		{"not json", `hello`, "a", "", false},
		{"empty input", ``, "a", "", false},
		{"truncated object", `{"a":`, "a", "", false},
		{"truncated string", `{"a":"unterminated`, "a", "", false},
		{"truncated nesting", `{"a":{"b":1`, "a", "", false},
		{"missing colon", `{"a" 1}`, "a", "", false},
		{"missing comma", `{"a":1 "b":2}`, "b", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := fieldValue([]byte(tt.data), tt.key)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v (got %q)", ok, tt.ok, got)
			}
			if ok && string(got) != tt.want {
				t.Errorf("value = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFieldValueDuplicateKey pins which one wins. Nothing on the wire sends
// duplicates, and a scanner that silently picked the second while
// encoding/json picked the first would be a difference worth knowing about.
func TestFieldValueDuplicateKey(t *testing.T) {
	got, ok := fieldValue([]byte(`{"e":"first","e":"second"}`), "e")
	if !ok || string(got) != `"first"` {
		t.Errorf("value = %q (ok=%v), want the first occurrence", got, ok)
	}
	// encoding/json takes the last. The two disagree, which is harmless here
	// because the payload is parsed by encoding/json afterwards either way,
	// and is recorded so that nobody discovers it as a surprise.
	var v struct {
		E string `json:"e"`
	}
	if err := json.Unmarshal([]byte(`{"e":"first","e":"second"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.E != "second" {
		t.Errorf("encoding/json took %q, the comment above is stale", v.E)
	}
}

func TestStringField(t *testing.T) {
	tests := []struct {
		name string
		data string
		key  string
		want string
		ok   bool
	}{
		{"plain", `{"e":"aggTrade"}`, "e", "aggTrade", true},
		{"empty string", `{"e":""}`, "e", "", true},
		{"not a string", `{"e":42}`, "e", "", false},
		{"absent", `{"x":"y"}`, "e", "", false},
		// An escape means unescaping, which is the parser's job. No field
		// this is used for ever carries one, so refusing is safe and keeps
		// the scanner from having to be right about \u sequences.
		{"escaped content", `{"e":"a\"b"}`, "e", "", false},
		{"unicode escape", `{"e":"a\u0041b"}`, "e", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := stringField([]byte(tt.data), tt.key)
			if ok != tt.ok || got != tt.want {
				t.Errorf("= %q, %v; want %q, %v", got, ok, tt.want, tt.ok)
			}
		})
	}
}

// TestFieldValueMatchesEncodingJSON is the differential test: over every
// captured frame, the scanner's answer for the two keys the decoder routes on
// must be the one encoding/json gives.
//
// A hand-written scanner earns its place only if it agrees with the library
// it replaced, and the only corpus worth checking that against is the one the
// venue actually sent.
func TestFieldValueMatchesEncodingJSON(t *testing.T) {
	frames, _ := referenceCorpusInternal(t)
	if len(frames) < 1000 {
		t.Skipf("only %d frames available", len(frames))
	}

	for i, payload := range frames {
		// The Discard field is not decoration. encoding/json falls back to a
		// case-insensitive match, so without an exact home for "E" it
		// unmarshals the event time, a number, into the "e" field and fails.
		// The production decoder carried the same decoy before this change,
		// and the scanner needs no such workaround: it matches exactly.
		var ref struct {
			Type    string          `json:"e"`
			Discard int64           `json:"E"`
			Stream  string          `json:"stream"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(payload, &ref); err != nil {
			t.Fatalf("frame %d does not parse: %v", i, err)
		}

		got, ok := fieldValue(payload, "data")
		if (len(ref.Data) > 0) != ok {
			t.Fatalf("frame %d: fieldValue(data) ok = %v, encoding/json found %d bytes", i, ok, len(ref.Data))
		}
		if ok && !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(ref.Data)) {
			t.Fatalf("frame %d: data span differs\n got %s\nwant %s", i, got, ref.Data)
		}
		if s, sok := stringField(payload, "stream"); sok != (ref.Stream != "") || s != ref.Stream {
			t.Fatalf("frame %d: stream = %q (%v), want %q", i, s, sok, ref.Stream)
		}

		inner := payload
		if ok {
			inner = got
		}
		var innerRef struct {
			Type    string `json:"e"`
			Discard int64  `json:"E"`
		}
		if err := json.Unmarshal(inner, &innerRef); err != nil {
			t.Fatalf("frame %d payload does not parse: %v", i, err)
		}
		typ, tok := stringField(inner, "e")
		if !tok || typ != innerRef.Type {
			t.Fatalf("frame %d: event type = %q (%v), want %q", i, typ, tok, innerRef.Type)
		}
	}
	t.Logf("%d captured frames agree with encoding/json on both fields", len(frames))
}

// FuzzFieldValue asserts the contract fieldValue actually offers, which is
// narrower than "the span is always valid JSON":
//
//   - on input that is a valid JSON object, a returned span is exactly what
//     encoding/json gives for that key, and a key that is present is found;
//   - on anything else, no promise at all.
//
// with the further caveat that a key spelled with an escape is not found,
// which the guard below encodes.
//
// The second half is deliberate and is why the first half is enough. The
// scanner is not a validator, and every caller hands what it returns to
// encoding/json, so a span read out of malformed input fails to parse rather
// than becoming a wrong event. The fuzzer found `{"e":A ` returning `A`,
// which is exactly that case: not valid JSON, refused downstream.
func FuzzFieldValue(f *testing.F) {
	for _, seed := range []string{
		`{"e":"depthUpdate","U":1,"u":2,"b":[["1.0","2.0"]],"a":[]}`,
		`{"stream":"btcusdt@depth","data":{"e":"depthUpdate"}}`,
		`{"a\"b":1,"e":"x"}`,
		`{ }`, `[]`, `null`, `{"a":`, `{"e":"A"}`, `{"e":A `, `{"\t":""}`,
	} {
		f.Add(seed, "e")
	}
	f.Fuzz(func(t *testing.T, data, key string) {
		got, ok := fieldValue([]byte(data), key)

		if !utf8.ValidString(data) || !utf8.ValidString(key) {
			// encoding/json replaces invalid UTF-8 in a key with U+FFFD when
			// it builds the map, so the two cannot be compared. The scanner
			// compares the bytes as written, which for a key that is not
			// valid UTF-8 is the more faithful of the two and is irrelevant
			// either way: every key looked up here is ASCII.
			return
		}
		var m map[string]json.RawMessage
		if !json.Valid([]byte(data)) || json.Unmarshal([]byte(data), &m) != nil {
			return // not a valid JSON object: nothing is promised
		}
		if n, ok := countTopLevelKeys(data); !ok || n != len(m) {
			// Duplicate keys, where the scanner takes the first and
			// encoding/json takes the last. Pinned in
			// TestFieldValueDuplicateKey; nothing on the wire sends them.
			return
		}
		want, present := m[key]
		if !ok {
			// Not finding a key that is present is correct when the key was
			// spelled with an escape: the scanner compares key bytes as
			// written. It is a bug only when the key appears literally.
			if present && strings.Contains(data, `"`+key+`"`) {
				t.Fatalf("key %q appears literally in %q and was not found", key, data)
			}
			return
		}
		if !present {
			t.Fatalf("found %q for a key %q that encoding/json says is absent from %q", got, key, data)
		}
		if ok && !bytes.Equal(bytes.TrimSpace(got), bytes.TrimSpace(want)) {
			t.Fatalf("key %q in %q: got %q, encoding/json gives %q", key, data, got, want)
		}
	})
}

// countTopLevelKeys counts the keys of a JSON object including repeats, so
// the fuzz property can exclude inputs where the two implementations
// legitimately disagree.
func countTopLevelKeys(data string) (int, bool) {
	dec := json.NewDecoder(strings.NewReader(data))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return 0, false
	}
	var n int
	for dec.More() {
		if _, err := dec.Token(); err != nil { // the key
			return 0, false
		}
		n++
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil { // the value
			return 0, false
		}
	}
	return n, true
}
