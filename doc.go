// Package reactive implements a deterministic reactive state, derived
// computation, and asynchronous side-effect scheduler.
//
// State changes are committed as atomic batches. Derived nodes are recomputed
// in dependency order at most once per batch, and effects are scheduled only
// after a successful commit. The scheduler bounds concurrency and queue depth,
// coalesces superseded effect runs, cancels timed-out or replaced runs, and
// discards stale results.
package reactive
