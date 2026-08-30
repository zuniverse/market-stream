// Package record implements raw websocket frame recording and replay.
//
// Source is the pipeline entry point interface. LiveSource wraps a real
// websocket connection; ReplaySource reads from a recorded file. Everything
// downstream is identical in both cases, which is what makes profiling numbers
// meaningful: the measured code path is the production code path (D5, D6).
//
// Recording format: each frame is a length-prefixed payload with a receive
// timestamp, stored in hourly files compressed with zstd. Normalised events
// are not recorded; recording post-decode would move the decoder outside the
// measured loop.
package record
