package model

// Instrument holds normalised metadata for one trading pair.
// PriceDecimals and QtyDecimals are the decimal exponents mentioned in D14:
// the values that must be passed to ParsePrice and ParseQty for this symbol.
type Instrument struct {
	Symbol        Symbol
	PriceDecimals int
	QtyDecimals   int
}
