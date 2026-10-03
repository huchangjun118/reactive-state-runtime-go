package reactive

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

// 批量提交：全部生效或全部不生效。
func TestBatchCommitAtomic(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 1)
	b := rt.State("b", 2)

	// 成功事务：两者同时生效。
	if err := rt.Update(func(tx *Tx) error {
		tx.Set(a, 10)
		tx.Set(b, 20)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if v, _ := rt.Get(a); v != 10 {
		t.Fatalf("a=%v", v)
	}
	if v, _ := rt.Get(b); v != 20 {
		t.Fatalf("b=%v", v)
	}

	// 失败事务：全部丢弃。
	wantErr := errors.New("boom")
	if err := rt.Update(func(tx *Tx) error {
		tx.Set(a, 99)
		tx.Set(b, 99)
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("err=%v", err)
	}
	if v, _ := rt.Get(a); v != 10 {
		t.Fatalf("a after failed tx=%v", v)
	}
	if v, _ := rt.Get(b); v != 20 {
		t.Fatalf("b after failed tx=%v", v)
	}

	// panic 事务：全部丢弃。
	if err := rt.Update(func(tx *Tx) error {
		tx.Set(a, 77)
		panic("oops")
	}); err == nil {
		t.Fatal("expected panic error")
	}
	if v, _ := rt.Get(a); v != 10 {
		t.Fatalf("a after panicked tx=%v", v)
	}
}

// 菱形依赖：每个派生节点一次提交只求值一次，结果与按依赖序重算一致。
func TestDiamondDependency(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 1)

	var bEvals, cEvals, dEvals int
	var dSeen [][2]int // d 每次求值读到的 (b,c) 对

	b := rt.Derive("b", func(s *Scope) (any, error) {
		bEvals++
		return s.Must(a).(int) + 1, nil
	})
	c := rt.Derive("c", func(s *Scope) (any, error) {
		cEvals++
		return s.Must(a).(int) * 2, nil
	})
	d := rt.Derive("d", func(s *Scope) (any, error) {
		dEvals++
		bv := s.Must(b).(int)
		cv := s.Must(c).(int)
		dSeen = append(dSeen, [2]int{bv, cv})
		return bv + cv, nil
	})

	if bEvals != 1 || cEvals != 1 || dEvals != 1 {
		t.Fatalf("initial evals: %d %d %d", bEvals, cEvals, dEvals)
	}

	if err := rt.Set(a, 5); err != nil {
		t.Fatal(err)
	}
	if bEvals != 2 || cEvals != 2 || dEvals != 2 {
		t.Fatalf("evals after one set: b=%d c=%d d=%d (each must be exactly 2)", bEvals, cEvals, dEvals)
	}
	// 结果一致：d = (5+1) + (5*2) = 16
	if v, _ := rt.Get(d); v != 16 {
		t.Fatalf("d=%v want 16", v)
	}
	// d 不得观察到中间态：它读到的 (b,c) 必须同属一个版本。
	for i, pair := range dSeen {
		want := [2]int{2, 2}
		if i > 0 {
			want = [2]int{6, 10}
		}
		if pair != want {
			t.Fatalf("d saw intermediate state %v at eval %d, want %v", pair, i, want)
		}
	}
}

// 相同初始状态与相同操作序列：求值次数、求值顺序、最终状态完全一致。
func TestDeterministicEvaluation(t *testing.T) {
	run := func() (order []string, finals []int) {
		rt := New(Options{})
		defer rt.Close()
		x := rt.State("x", 0)
		y := rt.State("y", 0)
		mk := func(name string, f func(s *Scope) (any, error)) Handle {
			return rt.Derive(name, func(s *Scope) (any, error) {
				order = append(order, name)
				return f(s)
			})
		}
		p := mk("p", func(s *Scope) (any, error) { return s.Must(x).(int) + 1, nil })
		q := mk("q", func(s *Scope) (any, error) { return s.Must(x).(int) + s.Must(y).(int), nil })
		r := mk("r", func(s *Scope) (any, error) { return s.Must(p).(int) * s.Must(q).(int), nil })
		for i := 1; i <= 5; i++ {
			if err := rt.Update(func(tx *Tx) error {
				tx.Set(x, i)
				tx.Set(y, i*2)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		for _, h := range []Handle{p, q, r} {
			v, _ := rt.Get(h)
			finals = append(finals, v.(int))
		}
		return order, finals
	}
	o1, f1 := run()
	for i := 0; i < 10; i++ {
		o2, f2 := run()
		if !reflect.DeepEqual(o1, o2) {
			t.Fatalf("evaluation order differs:\n%v\n%v", o1, o2)
		}
		if !reflect.DeepEqual(f1, f2) {
			t.Fatalf("final state differs: %v vs %v", f1, f2)
		}
	}
}

// 并发提交：全部生效，最终结果与串行执行一致（可串行化）。
func TestConcurrentCommits(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	counter := rt.State("counter", 0)
	sum := rt.Derive("sum", func(s *Scope) (any, error) {
		return s.Must(counter).(int) * 2, nil
	})

	const workers = 50
	const perWorker = 20
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := rt.Update(func(tx *Tx) error {
					v, _ := tx.Get(counter)
					tx.Set(counter, v.(int)+1)
					return nil
				}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	want := workers * perWorker
	if v, _ := rt.Get(counter); v != want {
		t.Fatalf("counter=%v want %d (lost update)", v, want)
	}
	if v, _ := rt.Get(sum); v != want*2 {
		t.Fatalf("sum=%v want %d", v, want*2)
	}
}

// 异常沿依赖链传播、可被观察；无关分支不受污染；恢复后继续一致。
func TestErrorPropagationAndRecovery(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 1)
	fail := rt.State("fail", false)

	good := rt.Derive("good", func(s *Scope) (any, error) {
		return s.Must(a).(int) + 1, nil
	})
	fragile := rt.Derive("fragile", func(s *Scope) (any, error) {
		if s.Must(fail).(bool) {
			return nil, errors.New("fragile failed")
		}
		return s.Must(a).(int) * 10, nil
	})
	downstream := rt.Derive("downstream", func(s *Scope) (any, error) {
		return s.Must(fragile).(int) + 100, nil
	})

	if err := rt.Set(fail, true); err != nil {
		t.Fatal(err)
	}
	// 失败节点保留旧值并报告错误。
	v, err := rt.Get(fragile)
	if err == nil || err.Error() != "fragile failed" {
		t.Fatalf("fragile err=%v", err)
	}
	if v != 10 {
		t.Fatalf("fragile keeps last good value, got %v", v)
	}
	// 下游读到传播的错误。
	if _, err := rt.Get(downstream); err == nil {
		t.Fatal("downstream should observe propagated error")
	}
	// 无关分支不受影响。
	if v, err := rt.Get(good); err != nil || v != 2 {
		t.Fatalf("good=%v err=%v", v, err)
	}
	// 恢复：新变更后重新求值并回到一致状态。
	if err := rt.Update(func(tx *Tx) error {
		tx.Set(fail, false)
		tx.Set(a, 3)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if v, err := rt.Get(fragile); err != nil || v != 30 {
		t.Fatalf("fragile recovered=%v err=%v", v, err)
	}
	if v, err := rt.Get(downstream); err != nil || v != 130 {
		t.Fatalf("downstream recovered=%v err=%v", v, err)
	}
}

// 派生计算 panic 被捕获为节点错误，不拖垮运行时。
func TestDerivedPanic(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 1)
	p := rt.Derive("p", func(s *Scope) (any, error) {
		if s.Must(a).(int) < 0 {
			panic("negative!")
		}
		return s.Must(a).(int), nil
	})
	if err := rt.Set(a, -1); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Get(p); err == nil {
		t.Fatal("expected panic converted to error")
	}
	if err := rt.Set(a, 2); err != nil {
		t.Fatal(err)
	}
	if v, err := rt.Get(p); err != nil || v != 2 {
		t.Fatalf("recovered p=%v err=%v", v, err)
	}
}

// 动态依赖：求值期间读到的集合即为新依赖集。
func TestDynamicDependencies(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	flag := rt.State("flag", true)
	x := rt.State("x", 1)
	y := rt.State("y", 100)

	var evals int
	d := rt.Derive("d", func(s *Scope) (any, error) {
		evals++
		if s.Must(flag).(bool) {
			return s.Must(x), nil
		}
		return s.Must(y), nil
	})
	if evals != 1 {
		t.Fatalf("evals=%d", evals)
	}
	// flag=true 时只依赖 x：改 y 不应触发重算。
	if err := rt.Set(y, 200); err != nil {
		t.Fatal(err)
	}
	if evals != 1 {
		t.Fatalf("evals after y change=%d, want 1 (y not a dependency)", evals)
	}
	// 切换分支后依赖集更新。
	if err := rt.Set(flag, false); err != nil {
		t.Fatal(err)
	}
	if evals != 2 {
		t.Fatalf("evals after flag change=%d", evals)
	}
	if v, _ := rt.Get(d); v != 200 {
		t.Fatalf("d=%v want 200", v)
	}
	if err := rt.Set(x, 9); err != nil {
		t.Fatal(err)
	}
	if evals != 2 {
		t.Fatalf("evals after x change=%d, want 2 (x no longer a dependency)", evals)
	}
	if err := rt.Set(y, 300); err != nil {
		t.Fatal(err)
	}
	if evals != 3 {
		t.Fatalf("evals after y change=%d, want 3", evals)
	}
}

// 注销：节点不再被求值/调度，运行时不再持有其引用。
func TestDispose(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 1)
	var evals int
	d := rt.Derive("d", func(s *Scope) (any, error) {
		evals++
		return s.Must(a).(int) * 2, nil
	})
	rt.Dispose(d)
	if err := rt.Set(a, 5); err != nil {
		t.Fatal(err)
	}
	if evals != 1 {
		t.Fatalf("disposed derived evaluated: evals=%d", evals)
	}
	if _, err := rt.Get(d); !errors.Is(err, ErrDisposed) {
		t.Fatalf("Get on disposed: err=%v", err)
	}
	// 重复注销安全。
	rt.Dispose(d)
}

// 注销被依赖的节点：订阅者重新求值并可通过动态依赖摘除它。
func TestDisposeDependency(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 1)
	b := rt.Derive("b", func(s *Scope) (any, error) {
		return s.Must(a).(int) + 1, nil
	})
	c := rt.Derive("c", func(s *Scope) (any, error) {
		v, err := s.Get(b)
		if errors.Is(err, ErrDisposed) {
			return s.Must(a).(int) * 100, nil // 摘除 b，改依赖 a
		}
		if err != nil {
			return nil, err
		}
		return v.(int) * 10, nil
	})
	if v, _ := rt.Get(c); v != 20 {
		t.Fatalf("c=%v", v)
	}
	rt.Dispose(b)
	if v, err := rt.Get(c); err != nil || v != 100 {
		t.Fatalf("c after dispose=%v err=%v", v, err)
	}
	// b 已注销后，c 的依赖已切换：改 a 仍触发 c。
	if err := rt.Set(a, 2); err != nil {
		t.Fatal(err)
	}
	if v, _ := rt.Get(c); v != 200 {
		t.Fatalf("c=%v want 200", v)
	}
}

// 关闭后拒绝新提交。
func TestClosedRuntime(t *testing.T) {
	rt := New(Options{})
	a := rt.State("a", 1)
	rt.Close()
	if err := rt.Set(a, 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v", err)
	}
}

// 读取方在提交过程中只能看到某个已提交的完整版本。
func TestConsistentSnapshots(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	a := rt.State("a", 0)
	b := rt.State("b", 0)
	// 派生节点在一次求值中读到 a、b 必须同属一个提交批次：
	// 本测试里每次提交都保持 a == b。
	rt.Derive("pair", func(s *Scope) (any, error) {
		av := s.Must(a).(int)
		bv := s.Must(b).(int)
		if av != bv {
			return nil, fmt.Errorf("torn read: a=%d b=%d", av, bv)
		}
		return av, nil
	})
	for i := 1; i <= 100; i++ {
		if err := rt.Update(func(tx *Tx) error {
			tx.Set(a, i)
			tx.Set(b, i)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}
