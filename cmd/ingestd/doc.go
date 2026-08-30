// Command ingestd is the live ingestion daemon.
//
// It connects to the Binance public websocket feed, normalises trade and
// order book events, maintains per-symbol books in sharded goroutines, and
// fans out to registered subscribers through the publisher. A structured-log
// subscriber writes to stdout; a Prometheus metrics endpoint is available on
// -metrics-addr.
//
// Run with -help for the full flag list. Flag names are a public commitment
// under D11 and D19.
package main
