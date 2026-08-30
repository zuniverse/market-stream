// Package model defines the canonical types shared by all pipeline stages.
//
// Event is a tagged struct carrying a Kind discriminator and one of three
// payload types: Trade, BookDelta, or Snapshot. The tagged struct avoids
// interface boxing on the hot path and allows exhaustive switches to be
// checked statically (D15).
//
// Price is type Price int64, a decimal fixed-point value. The exponent is not
// stored in the value; it is an attribute of the instrument and is tracked
// separately. Prices from different symbols must never be compared directly
// (D14).
//
// Symbol is the normalised pair string in the form "BASE-QUOTE" (e.g.
// "BTC-USDT"). All exchange-specific symbol formats are converted to this
// form at the decode boundary (D16).
package model
