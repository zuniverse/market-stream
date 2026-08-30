// Command loadgen is DEFERRED (v0.1 backlog).
//
// It will provide fault injection and synthetic overload: forced sequence
// gaps, mid-burst disconnection, and a single symbol pushed beyond any real
// market rate, to verify that sharding does not help in that case and that
// backpressure takes over cleanly (D3).
package main
