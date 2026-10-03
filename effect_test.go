package reactive

import (
	"context"
	"errors"
	"testing"
	"time"
)

func waitValue[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		var zero T
		t.Fatalf("timed out waiting for %v", zero)
		return zero
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestEffectReceivesCommittedValuesInOrder(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	first := NewState(rt, 0)
	second := NewState(rt, 0)
	events := make(chan int, 8)
	NewEffect(rt, func(_ context.Context, v Values) error {
		events <- 1
		return nil
	}, first)
	NewEffect(rt, func(_ context.Context, v Values) error {
		events <- 2
		return nil
	}, second)
	waitValue(t, events, time.Second)
	waitValue(t, events, time.Second)

	version, err := rt.Update(func(tx *Tx) error {
		first.Set(tx, 10)
		second.Set(tx, 20)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("version = %d, want 1", version)
	}
	if got := waitValue(t, events, time.Second); got != 1 {
		t.Fatalf("first event = %d, want 1", got)
	}
	if got := waitValue(t, events, time.Second); got != 2 {
		t.Fatalf("second event = %d, want 2", got)
	}
}

func TestStaleEffectResultIsDiscarded(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	events := make(chan int, 4)
	errs := make(chan error, 4)
	rt.observer.OnEffectError = func(_ ID, _ uint64, err error) {
		errs <- err
	}
	NewEffect(rt, func(_ context.Context, v Values) error {
		if v[0].(int) == 1 {
			close(started)
			<-release
			return errors.New("stale result")
		}
		events <- v[0].(int)
		return nil
	}, state)
	waitValue(t, events, time.Second)

	if _, err := rt.Update(func(tx *Tx) error { state.Set(tx, 1); return nil }); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, started, time.Second)
	if _, err := rt.Update(func(tx *Tx) error { state.Set(tx, 2); return nil }); err != nil {
		t.Fatal(err)
	}
	close(release)

	if got := waitValue(t, events, time.Second); got != 2 {
		t.Fatalf("event = %d, want 2", got)
	}
	select {
	case err := <-errs:
		t.Fatalf("stale error was observed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := rt.metrics.EffectErrors.Load(); got != 0 {
		t.Fatalf("effect errors = %d, want 0", got)
	}
}

func TestEffectSkipsFailedDependencyAndRecovers(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10, ErrorPolicy: ErrorPolicyIsolate})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	failing := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		if v[0].(int) > 0 {
			return 0, errors.New("bad value")
		}
		return v[0].(int), nil
	}, state)
	events := make(chan int, 4)
	NewEffect(rt, func(_ context.Context, v Values) error {
		events <- v[0].(int)
		return nil
	}, failing)
	if got := waitValue(t, events, time.Second); got != 0 {
		t.Fatalf("initial event = %d, want 0", got)
	}

	if _, err := rt.Update(func(tx *Tx) error { state.Set(tx, 1); return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-events:
		t.Fatalf("effect ran with failed dependency: %d", got)
	case <-time.After(50 * time.Millisecond):
	}
	if got := rt.metrics.EffectSkipped.Load(); got != 1 {
		t.Fatalf("skipped effects = %d, want 1", got)
	}

	if _, err := rt.Update(func(tx *Tx) error { state.Set(tx, 0); return nil }); err != nil {
		t.Fatal(err)
	}
	if got := waitValue(t, events, time.Second); got != 0 {
		t.Fatalf("recovery event = %d, want 0", got)
	}
}

func TestEffectTimeoutAndCloseCancel(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10, EffectTimeout: 50 * time.Millisecond})
	errs := make(chan error, 1)
	rt.observer.OnEffectError = func(_ ID, _ uint64, err error) { errs <- err }
	NewEffect(rt, func(ctx context.Context, _ Values) error {
		<-ctx.Done()
		return ctx.Err()
	})
	err := waitValue(t, errs, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want DeadlineExceeded", err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEffectQueueRejectionIsObservable(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 1, QueuePolicy: QueuePolicyReject})
	t.Cleanup(func() { _ = rt.Close() })

	block := make(chan struct{})
	started := make(chan struct{})
	events := make(chan int, 4)
	state := NewState(rt, 0)
	NewEffect(rt, func(_ context.Context, v Values) error {
		if v[0].(int) == 0 {
			close(started)
			<-block
			return nil
		}
		events <- v[0].(int)
		return nil
	}, state)
	waitClosed(t, started, time.Second)

	NewEffect(rt, func(_ context.Context, _ Values) error {
		events <- 2
		return nil
	}, state)
	NewEffect(rt, func(_ context.Context, _ Values) error {
		events <- 3
		return nil
	}, state)

	close(block)
	if got := waitValue(t, events, time.Second); got != 2 {
		t.Fatalf("queued event = %d, want 2", got)
	}
	select {
	case v := <-events:
		t.Fatalf("rejected effect ran: %d", v)
	case <-time.After(50 * time.Millisecond):
	}
	if got := rt.metrics.EffectDrops.Load(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}
}

func TestSlowEffectDoesNotBlockUnrelatedBranch(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 2, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	slowState := NewState(rt, 0)
	fastState := NewState(rt, 0)
	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	fastEvents := make(chan int, 4)
	NewEffect(rt, func(_ context.Context, v Values) error {
		if v[0].(int) == 1 {
			close(slowStarted)
			<-releaseSlow
		}
		return nil
	}, slowState)
	NewEffect(rt, func(_ context.Context, v Values) error {
		fastEvents <- v[0].(int)
		return nil
	}, fastState)
	waitValue(t, fastEvents, time.Second)

	if _, err := rt.Update(func(tx *Tx) error { slowState.Set(tx, 1); return nil }); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, slowStarted, time.Second)
	if _, err := rt.Update(func(tx *Tx) error { fastState.Set(tx, 1); return nil }); err != nil {
		t.Fatal(err)
	}
	if got := waitValue(t, fastEvents, time.Second); got != 1 {
		t.Fatalf("fast event = %d, want 1", got)
	}
	close(releaseSlow)
}
