// Package binance implements the exchange transport and decoding for Binance.
//
// It owns the websocket connection, reconnection with exponential backoff and
// jitter, the serverShutdown reconnect trigger, frame decoding, and
// normalisation of raw payloads into model.Event values. Symbol names are
// split into BASE-QUOTE using the exchangeInfo REST endpoint fetched once at
// startup (D16).
//
// Nothing from this package is referenced outside internal/exchange/. All
// Binance-specific types remain here. Adding a second exchange must not
// require editing any file in this package (D11).
package binance
