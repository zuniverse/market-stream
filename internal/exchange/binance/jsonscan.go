package binance

// A minimal scanner for the two fields the routing decision needs, so that
// the frame is validated once rather than three times.
//
// D8 chose encoding/json for the whole decoder and said a hand-written
// scanner is worth its maintenance cost "only where a profile shows it pays,
// and only with a measured figure attached". The M4 profile showed it pays
// here and nowhere else yet: json.Unmarshal runs checkValid over the entire
// payload before it walks it, and Decode did that twice per frame, once on
// the combined-stream wrapper to read two fields and once on the payload.
// checkValid alone was a quarter of the whole pipeline's CPU.
//
// What is written here is deliberately not a JSON parser. It walks the top
// level of an object to locate one key's raw value, and it gives up rather
// than guess whenever the input is not the shape it expects. Nothing trusts
// its output: the value it returns is handed to encoding/json, which
// validates it properly, so a frame this scanner reads wrongly fails to
// decode rather than decoding into something wrong.

// fieldValue returns the raw value of a top-level key of a JSON object.
//
// ok is false when data is not an object, when the key is absent, or when the
// scanner meets anything it is not sure about.
//
// Keys are compared as they are written, so a key spelled with an escape,
// "\u0065" for "e", is not found. Nothing this is used for can be spelled
// that way, and the failure direction is right: an unrecognised frame is
// refused rather than decoded into the wrong thing.
//
// The contract is narrow on purpose. On input that is a valid JSON object
// whose keys need no unescaping, the span returned is exactly what
// encoding/json would give for that key, and a key that is present is found.
// On anything else nothing is promised:
// the span may not even be valid JSON. That is safe because every caller
// hands the span to encoding/json, so a value read out of a malformed frame
// fails to parse rather than becoming a wrong event.
func fieldValue(data []byte, key string) (value []byte, ok bool) {
	i := skipSpace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return nil, false
	}
	i++
	for {
		i = skipSpace(data, i)
		if i >= len(data) {
			return nil, false
		}
		if data[i] == '}' {
			return nil, false // end of object, key absent
		}
		if data[i] != '"' {
			return nil, false
		}
		name, next, ok := scanString(data, i)
		if !ok {
			return nil, false
		}
		i = skipSpace(data, next)
		if i >= len(data) || data[i] != ':' {
			return nil, false
		}
		start := skipSpace(data, i+1)
		end, ok := skipValue(data, start)
		if !ok {
			return nil, false
		}
		if string(name) == key {
			return data[start:end], true
		}
		i = skipSpace(data, end)
		if i >= len(data) {
			return nil, false
		}
		switch data[i] {
		case ',':
			i++
		case '}':
			return nil, false
		default:
			return nil, false
		}
	}
}

// stringField returns the value of a top-level key that holds a string,
// unquoted. ok is false if the key is absent or its value is not a string, or
// if the string contains an escape: unescaping is the parser's job, and no
// field this is used for ever carries one.
func stringField(data []byte, key string) (string, bool) {
	raw, ok := fieldValue(data, key)
	if !ok || len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return "", false
	}
	inner := raw[1 : len(raw)-1]
	for _, c := range inner {
		if c == '\\' {
			return "", false
		}
	}
	return string(inner), true
}

func skipSpace(data []byte, i int) int {
	for i < len(data) {
		switch data[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// scanString consumes a quoted string starting at data[i], which must be a
// quote, and returns its contents and the index just past the closing quote.
func scanString(data []byte, i int) (content []byte, next int, ok bool) {
	i++ // opening quote
	start := i
	for i < len(data) {
		switch data[i] {
		case '\\':
			i += 2 // an escape and whatever it escapes
		case '"':
			return data[start:i], i + 1, true
		default:
			i++
		}
	}
	return nil, 0, false
}

// skipValue returns the index just past the JSON value starting at data[i].
//
// Nesting is tracked by depth rather than by recursion, so a pathological
// frame costs a counter rather than a stack.
func skipValue(data []byte, i int) (end int, ok bool) {
	if i >= len(data) {
		return 0, false
	}
	switch data[i] {
	case '"':
		_, next, ok := scanString(data, i)
		return next, ok
	case '{', '[':
		depth := 0
		for i < len(data) {
			switch data[i] {
			case '{', '[':
				depth++
				i++
			case '}', ']':
				depth--
				i++
				if depth == 0 {
					return i, true
				}
			case '"':
				_, next, ok := scanString(data, i)
				if !ok {
					return 0, false
				}
				i = next
			default:
				i++
			}
		}
		return 0, false
	default:
		// A number, true, false or null: everything up to the next structural
		// character. The value is handed to encoding/json, which is what
		// decides whether it was one of those.
		start := i
		for i < len(data) {
			switch data[i] {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				if i == start {
					return 0, false
				}
				return i, true
			default:
				i++
			}
		}
		return 0, false
	}
}
