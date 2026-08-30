package model

import (
	"errors"
	"math"
	"math/bits"
	"strconv"
	"strings"
)

// Sentinel errors returned by ParseFixed, ParsePrice, and ParseQty.
var (
	ErrInvalidSyntax   = errors.New("model: invalid decimal syntax")
	ErrOverflow        = errors.New("model: value overflows int64")
	ErrTooManyDecimals = errors.New("model: too many fractional digits")
)

// ParseFixed converts the decimal string s to a fixed-point int64 scaled to
// decimals fractional digits. A leading '-' is accepted; scientific notation
// is not. Fewer fractional digits than decimals are right-padded with zeros.
//
//	ParseFixed("1234.56", 2) -> 123456
//	ParseFixed("1234",    2) -> 123400
//	ParseFixed("0.5",     8) -> 50000000
func ParseFixed(s string, decimals int) (int64, error) {
	if len(s) == 0 {
		return 0, ErrInvalidSyntax
	}

	neg := false
	rest := s
	if rest[0] == '-' {
		neg = true
		rest = rest[1:]
		if len(rest) == 0 {
			return 0, ErrInvalidSyntax
		}
	}

	intPart, fracPart, _ := strings.Cut(rest, ".")
	if intPart == "" {
		intPart = "0"
	}

	if len(fracPart) > decimals {
		return 0, ErrTooManyDecimals
	}
	if n := decimals - len(fracPart); n > 0 {
		fracPart += strings.Repeat("0", n)
	}

	val, err := strconv.ParseUint(intPart+fracPart, 10, 64)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) && numErr.Err == strconv.ErrRange {
			return 0, ErrOverflow
		}
		return 0, ErrInvalidSyntax
	}

	if neg {
		if val > uint64(math.MaxInt64)+1 {
			return 0, ErrOverflow
		}
		if val == uint64(math.MaxInt64)+1 {
			return math.MinInt64, nil
		}
		return -int64(val), nil
	}
	if val > uint64(math.MaxInt64) {
		return 0, ErrOverflow
	}
	return int64(val), nil
}

// ParsePrice parses s as a Price with decimals fractional digits.
func ParsePrice(s string, decimals int) (Price, error) {
	v, err := ParseFixed(s, decimals)
	return Price(v), err
}

// ParseQty parses s as a Qty with decimals fractional digits.
func ParseQty(s string, decimals int) (Qty, error) {
	v, err := ParseFixed(s, decimals)
	return Qty(v), err
}

// Notional multiplies p by q and returns the result along with an overflow
// flag. p and q must be non-negative; negative inputs are treated as overflow.
func Notional(p Price, q Qty) (int64, bool) {
	if p < 0 || q < 0 {
		return 0, true
	}
	hi, lo := bits.Mul64(uint64(p), uint64(q))
	if hi != 0 || lo > uint64(math.MaxInt64) {
		return 0, true
	}
	return int64(lo), false
}
