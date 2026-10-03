package reactive

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAddDependencyUsesNewInputAndTriggersEffect(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	base := NewState(rt, 1)
	extra := NewState(rt, 10)
	derived := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		if len(v) != 2 {
			return 0, errors.New("expected two inputs")
		}
		return v[0].(int) + v[1].(int), nil
	}, base)
	events := make(chan int, 4)
	NewEffect(rt, func(_ context.Context, v Values) error {
		if len(v) == 1 {
			events <- v[0].(int)
		}
		return nil
	}, derived)

	if err := AddDependency(derived, extra); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := derived.Get(); got != 11 {
		t.Fatalf("derived = %d after adding dependency, want 11", got)
	}
	if got := waitValue(t, events, time.Second); got != 11 {
		t.Fatalf("effect event = %d, want 11", got)
	}

	if _, err := rt.Update(func(tx *Tx) error {
		base.Set(tx, 2)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := derived.Get(); got != 12 {
		t.Fatalf("derived = %d, want 12", got)
	}
	if got := waitValue(t, events, time.Second); got != 12 {
		t.Fatalf("effect event = %d, want 12", got)
	}
}

func TestAddAndRemoveDependencyEdge(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	base := NewState(rt, 1)
	extra := NewState(rt, 10)
	derived := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		if len(v) == 1 {
			return v[0].(int), nil
		}
		return v[0].(int) + v[1].(int), nil
	}, base)
	events := make(chan int, 4)
	NewEffect(rt, func(_ context.Context, v Values) error {
		events <- v[0].(int)
		return nil
	}, derived)
	if got := waitValue(t, events, time.Second); got != 1 {
		t.Fatalf("initial event = %d, want 1", got)
	}

	if err := AddDependency(derived, extra); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := derived.Get(); got != 11 {
		t.Fatalf("derived with two deps = %d, want 11", got)
	}
	if got := waitValue(t, events, time.Second); got != 11 {
		t.Fatalf("add-dependency event = %d, want 11", got)
	}

	if err := RemoveDependency(derived, extra); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := derived.Get(); got != 1 {
		t.Fatalf("derived after edge removal = %d, want 1", got)
	}
	if got := waitValue(t, events, time.Second); got != 1 {
		t.Fatalf("remove-dependency event = %d, want 1", got)
	}
}

func TestRemoveEffectDoesNotRunAfterChange(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	events := make(chan int, 4)
	effect := NewEffect(rt, func(_ context.Context, v Values) error {
		events <- v[0].(int)
		return nil
	}, state)
	waitValue(t, events, time.Second)

	if err := Remove(effect); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Update(func(tx *Tx) error {
		state.Set(tx, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-events:
		t.Fatalf("removed effect ran with %d", v)
	case <-time.After(50 * time.Millisecond):
	}
	if len(rt.nodes) != 1 {
		t.Fatalf("node count = %d, want 1", len(rt.nodes))
	}
}

func TestRemoveDerivedDetachesDependentEffect(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	derived := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		return v[0].(int) + 1, nil
	}, state)
	events := make(chan int, 4)
	NewEffect(rt, func(_ context.Context, v Values) error {
		if len(v) == 1 {
			events <- v[0].(int)
		}
		return nil
	}, derived)
	waitValue(t, events, time.Second)

	if err := Remove(derived); err != nil {
		t.Fatal(err)
	}

	if _, err := rt.Update(func(tx *Tx) error {
		state.Set(tx, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-events:
		t.Fatalf("detached effect ran with %d", v)
	case <-time.After(50 * time.Millisecond):
	}
	if len(rt.nodes) != 2 {
		t.Fatalf("node count = %d, want 2", len(rt.nodes))
	}
}

func TestRemoveStateWithDependentsFails(t *testing.T) {
	rt := NewRuntime(Config{})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	NewDerived(rt, func(_ context.Context, v Values) (int, error) { return v[0].(int), nil }, state)
	if err := Remove(state); err == nil {
		t.Fatal("expected removal error")
	}
}

func TestDependencyCycleIsRejected(t *testing.T) {
	rt := NewRuntime(Config{})
	t.Cleanup(func() { _ = rt.Close() })

	first := NewState(rt, 0)
	second := NewDerived(rt, func(_ context.Context, v Values) (int, error) { return v[0].(int), nil }, first)
	third := NewDerived(rt, func(_ context.Context, v Values) (int, error) { return v[0].(int), nil }, second)
	if err := AddDependency(first, third); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestLongRunningGraphHasNoUnboundedGrowth(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 2, MaxEffectQueue: 100})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	var evaluations atomic.Int64
	derived := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		return v[0].(int) + 1, nil
	}, state)
	events := make(chan int, 1024)
	NewEffect(rt, func(_ context.Context, v Values) error {
		events <- v[0].(int)
		return nil
	}, derived)
	waitValue(t, events, time.Second)

	for i := 1; i <= 200; i++ {
		if _, err := rt.Update(func(tx *Tx) error {
			state.Set(tx, i)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for final effect event")
		}
		select {
		case got := <-events:
			if got == 201 {
				goto finalEvent
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for effect event")
		}
	}
finalEvent:
	select {
	case v := <-events:
		t.Fatalf("coalesced effect ran again with %d", v)
	case <-time.After(50 * time.Millisecond):
	}
	if got := len(rt.nodes); got != 3 {
		t.Fatalf("node count = %d, want 3", got)
	}
	if got, _, _ := derived.Get(); got != 201 {
		t.Fatalf("derived = %d, want 201", got)
	}
}

func TestConcurrentCommitsScheduleEffectsInVersionOrder(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 1024})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	events := make(chan int, 1024)
	NewEffect(rt, func(_ context.Context, v Values) error {
		if v[0].(int) == 0 {
			close(started)
			<-release
			return nil
		}
		events <- v[0].(int)
		return nil
	}, state)
	waitClosed(t, started, time.Second)

	const count = 100
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 1; i <= count; i++ {
		go func(value int) {
			defer wg.Done()
			_, _ = rt.Update(func(tx *Tx) error {
				state.Set(tx, value)
				return nil
			})
		}(i)
	}
	wg.Wait()
	close(release)
	final, _ := state.Get()

	if got := waitValue(t, events, time.Second); got != final {
		t.Fatalf("final effect value = %d, want final state %d", got, final)
	}
	select {
	case got := <-events:
		t.Fatalf("unexpected intermediate effect value %d", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestConcurrentCommitsAreAtomic(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 4, MaxEffectQueue: 100})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	var evaluations atomic.Int64
	NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		return v[0].(int), nil
	}, state)

	const count = 100
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 1; i <= count; i++ {
		go func(value int) {
			defer wg.Done()
			if _, err := rt.Update(func(tx *Tx) error {
				state.Set(tx, value)
				return nil
			}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	version := rt.currentVersion()
	if version != count {
		t.Fatalf("version = %d, want %d", version, count)
	}
	value, _ := state.Get()
	if value < 1 || value > count {
		t.Fatalf("final value = %d, want between 1 and %d", value, count)
	}
	if got := evaluations.Load(); got != count+1 {
		t.Fatalf("evaluations = %d, want %d", got, count+1)
	}
}
