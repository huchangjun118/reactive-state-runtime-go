package reactive

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestBatchEvaluatesDiamondOnce(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	a := NewState(rt, 1)
	var evaluations atomic.Int64
	double := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		return v[0].(int) * 2, nil
	}, a)
	triple := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		return v[0].(int) * 3, nil
	}, a)
	batch := false
	sum := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		if batch && (v[0].(int) != 20 || v[1].(int) != 30) {
			t.Errorf("derived saw intermediate values: %v", v)
		}
		return v[0].(int) + v[1].(int), nil
	}, double, triple)
	product := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		return v[0].(int) * v[1].(int), nil
	}, double, triple)

	before := evaluations.Load()
	batch = true
	version, err := rt.Update(func(tx *Tx) error {
		a.Set(tx, 10)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("version = %d, want 1", version)
	}
	if got := evaluations.Load() - before; got != 4 {
		t.Fatalf("evaluations = %d, want 4", got)
	}
	if got, _, _ := sum.Get(); got != 50 {
		t.Fatalf("sum = %d, want 50", got)
	}
	if got, _, _ := product.Get(); got != 600 {
		t.Fatalf("product = %d, want 600", got)
	}
}

func TestBatchUsesLatestValuesForAllStates(t *testing.T) {
	rt := NewRuntime(Config{MaxConcurrentEffects: 1, MaxEffectQueue: 10})
	t.Cleanup(func() { _ = rt.Close() })

	first := NewState(rt, 1)
	second := NewState(rt, 2)
	var evaluations atomic.Int64
	derived := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		evaluations.Add(1)
		return v[0].(int) + v[1].(int), nil
	}, first, second)

	before := evaluations.Load()
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
	if got := evaluations.Load() - before; got != 1 {
		t.Fatalf("evaluations = %d, want 1", got)
	}
	if got, _, _ := derived.Get(); got != 30 {
		t.Fatalf("derived = %d, want 30", got)
	}
}

func TestTransactionFunctionErrorPublishesNothing(t *testing.T) {
	rt := NewRuntime(Config{})
	t.Cleanup(func() { _ = rt.Close() })

	state := NewState(rt, 0)
	version, err := rt.Update(func(tx *Tx) error {
		state.Set(tx, 10)
		return errors.New("abort")
	})
	if err == nil || err.Error() != "abort" {
		t.Fatalf("error = %v, want abort", err)
	}
	if version != 0 {
		t.Fatalf("version = %d, want 0", version)
	}
	if got, _ := state.Get(); got != 0 {
		t.Fatalf("state = %d, want 0", got)
	}
}

func TestFailFastRollsBackCompleteTransaction(t *testing.T) {
	rt := NewRuntime(Config{ErrorPolicy: ErrorPolicyFailFast})
	t.Cleanup(func() { _ = rt.Close() })

	good := NewState(rt, 0)
	bad := NewState(rt, 0)
	failing := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		if v[0].(int) > 0 {
			return 0, errors.New("bad value")
		}
		return 0, nil
	}, bad)

	version, err := rt.Update(func(tx *Tx) error {
		good.Set(tx, 10)
		bad.Set(tx, 1)
		return nil
	})
	if err == nil {
		t.Fatal("expected transaction error")
	}
	if version != 0 {
		t.Fatalf("version = %d, want 0", version)
	}
	if got, _ := good.Get(); got != 0 {
		t.Fatalf("good = %d, want 0", got)
	}
	if got, _ := bad.Get(); got != 0 {
		t.Fatalf("bad = %d, want 0", got)
	}
	if _, _, derr := failing.Get(); derr != nil {
		t.Fatalf("failing error = %v, want nil after rollback", derr)
	}
	if got := rt.currentVersion(); got != 0 {
		t.Fatalf("committed version = %d, want 0", got)
	}
}

func TestIsolateCommitsValidBranchAndRecovers(t *testing.T) {
	rt := NewRuntime(Config{ErrorPolicy: ErrorPolicyIsolate})
	t.Cleanup(func() { _ = rt.Close() })

	good := NewState(rt, 0)
	bad := NewState(rt, 0)
	failing := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		if v[0].(int) > 0 {
			return 0, errors.New("bad value")
		}
		return 7, nil
	}, bad)
	unrelated := NewDerived(rt, func(_ context.Context, v Values) (int, error) {
		return v[0].(int) + 1, nil
	}, good)

	version, err := rt.Update(func(tx *Tx) error {
		good.Set(tx, 10)
		bad.Set(tx, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("version = %d, want 1", version)
	}
	if got, _ := good.Get(); got != 10 {
		t.Fatalf("good = %d, want 10", got)
	}
	if got, _, _ := unrelated.Get(); got != 11 {
		t.Fatalf("unrelated = %d, want 11", got)
	}
	if _, _, derr := failing.Get(); derr == nil {
		t.Fatal("expected isolated derived error")
	}

	version, err = rt.Update(func(tx *Tx) error {
		bad.Set(tx, 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("version = %d, want 2", version)
	}
	if got, _, derr := failing.Get(); got != 7 || derr != nil {
		t.Fatalf("failing = (%d, %v), want (7, nil)", got, derr)
	}
}
