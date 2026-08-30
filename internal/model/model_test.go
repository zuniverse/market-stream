package model_test

import (
	"errors"
	"math"
	"testing"

	"github.com/zuniverse/market-stream/internal/model"
)

func TestParseFixed(t *testing.T) {
	tests := []struct {
		s        string
		decimals int
		want     int64
		wantErr  error
	}{
		// Basic cases.
		{"1234.56", 2, 123456, nil},
		{"1234", 2, 123400, nil},
		{"1234.5", 2, 123450, nil},
		{"0", 0, 0, nil},
		{"0.00", 2, 0, nil},
		{"1234", 0, 1234, nil},
		{"0.00001234", 8, 1234, nil},
		{"1.00000000", 8, 100_000_000, nil},

		// Leading dot.
		{".5", 1, 5, nil},
		{".50", 2, 50, nil},

		// Negative values.
		{"-1234.56", 2, -123456, nil},
		{"-0.5", 1, -5, nil},

		// int64 boundary values.
		{"9223372036854775807", 0, math.MaxInt64, nil},
		{"-9223372036854775808", 0, math.MinInt64, nil},

		// Overflow: one past the boundary.
		{"9223372036854775808", 0, 0, model.ErrOverflow},
		{"-9223372036854775809", 0, 0, model.ErrOverflow},
		// Overflow via large magnitude (exceeds uint64).
		{"99999999999999999999", 0, 0, model.ErrOverflow},
		// Overflow due to decimal scaling: 92233720368547758.08 -> 9223372036854775808 > MaxInt64.
		{"92233720368547758.08", 2, 0, model.ErrOverflow},

		// Too many fractional digits.
		{"1234.567", 2, 0, model.ErrTooManyDecimals},
		{"0.001", 2, 0, model.ErrTooManyDecimals},

		// Invalid syntax.
		{"", 2, 0, model.ErrInvalidSyntax},
		{"-", 2, 0, model.ErrInvalidSyntax},
		{"abc", 2, 0, model.ErrInvalidSyntax},
		{"12.3a", 2, 0, model.ErrInvalidSyntax},
		{"1e5", 0, 0, model.ErrInvalidSyntax},
	}

	for _, tc := range tests {
		got, err := model.ParseFixed(tc.s, tc.decimals)
		if tc.wantErr != nil {
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("ParseFixed(%q, %d) error = %v, want %v", tc.s, tc.decimals, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseFixed(%q, %d) unexpected error: %v", tc.s, tc.decimals, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseFixed(%q, %d) = %d, want %d", tc.s, tc.decimals, got, tc.want)
		}
	}
}

func TestParsePrice(t *testing.T) {
	p, err := model.ParsePrice("50000.00", 2)
	if err != nil {
		t.Fatal(err)
	}
	if p != model.Price(5_000_000) {
		t.Errorf("got %d, want 5000000", p)
	}
}

func TestParseQty(t *testing.T) {
	q, err := model.ParseQty("0.10000000", 8)
	if err != nil {
		t.Fatal(err)
	}
	if q != model.Qty(10_000_000) {
		t.Errorf("got %d, want 10000000", q)
	}
}

func TestNotional(t *testing.T) {
	tests := []struct {
		name     string
		p        model.Price
		q        model.Qty
		want     int64
		overflow bool
	}{
		{"zero price", 0, model.Qty(math.MaxInt64), 0, false},
		{"zero qty", model.Price(math.MaxInt64), 0, 0, false},
		{"unit", 1, 1, 1, false},
		// BTC @ $50000 (2 decimals) x 0.1 BTC (8 decimals): 5000000 * 10000000 = 5*10^13.
		{"typical crypto", model.Price(5_000_000), model.Qty(10_000_000), 50_000_000_000_000, false},
		// MaxInt64 * 1 fits exactly.
		{"max price unit qty", model.Price(math.MaxInt64), 1, math.MaxInt64, false},
		// MaxInt64 * 2 overflows.
		{"max price double qty", model.Price(math.MaxInt64), 2, 0, true},
		// Large values whose product exceeds MaxInt64.
		{"large overflow", model.Price(1_000_000_000), model.Qty(10_000_000_000), 0, true},
		// Negative inputs are treated as overflow.
		{"negative price", model.Price(-1), 1, 0, true},
		{"negative qty", 1, model.Qty(-1), 0, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ov := model.Notional(tc.p, tc.q)
			if ov != tc.overflow {
				t.Errorf("overflow = %v, want %v", ov, tc.overflow)
				return
			}
			if !ov && got != tc.want {
				t.Errorf("result = %d, want %d", got, tc.want)
			}
		})
	}
}
