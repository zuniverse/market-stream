// Command replay runs a recorded frame file through the full pipeline.
//
// It is the primary vehicle for profiling and correctness testing: because
// LiveSource and ReplaySource both implement record.Source, the measured code
// path is identical to the production code path (D5, D6).
package main
