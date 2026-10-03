package reactive

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 异步乱序：较早发起但较晚返回的结果必须被丢弃。
func TestStaleResultDiscarded(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	src := rt.State("src", 0)
	out := rt.State("out", -1)

	var firstStarted, firstMayFinish = make(chan struct{}), make(chan struct{})
	var call atomic.Int64
	rt.Effect("eff", []Handle{src}, func(c *EffectContext) error {
		n := call.Add(1)
		v, _ := c.Get(src)
		if n == 1 {
			close(firstStarted)
			<-firstMayFinish // 第一次执行被阻塞，直到第二次触发后
		}
		return c.Set(out, v.(int)*10)
	})

	// 初始触发（gen=1）开始执行并阻塞。
	<-firstStarted
	// 第二次触发：取消第一次，pending 重跑。
	if err := rt.Set(src, 7); err != nil {
		t.Fatal(err)
	}
	close(firstMayFinish)
	rt.Flush()

	// 第一次（旧值 0）的写回必须被丢弃，最终只能看到最新代次的结果 70。
	if v, _ := rt.Get(out); v != 70 {
		t.Fatalf("out=%v want 70 (stale write leaked)", v)
	}
}

// 同一依赖只允许一个在途执行。
func TestSingleInflight(t *testing.T) {
	rt := New(Options{MaxConcurrency: 8})
	defer rt.Close()
	src := rt.State("src", 0)

	var inflight, maxInflight atomic.Int64
	var done atomic.Int64
	rt.Effect("eff", []Handle{src}, func(c *EffectContext) error {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		done.Add(1)
		return nil
	})
	for i := 1; i <= 10; i++ {
		if err := rt.Set(src, i); err != nil {
			t.Fatal(err)
		}
	}
	rt.Flush()
	if m := maxInflight.Load(); m != 1 {
		t.Fatalf("max concurrent runs of one effect = %d, want 1", m)
	}
	if done.Load() < 1 {
		t.Fatal("effect never ran")
	}
}

// 超时：过期结果不得写回状态。
func TestTimeoutDiscardsResult(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	src := rt.State("src", 0)
	out := rt.State("out", -1)

	rt.EffectWithOptions("slow", []Handle{src}, EffectOptions{Timeout: 20 * time.Millisecond},
		func(c *EffectContext) error {
			select {
			case <-c.Done:
				return errors.New("cancelled")
			case <-time.After(200 * time.Millisecond):
			}
			// 超时后写回必须失败。
			return c.Set(out, 999)
		})

	rt.Flush()
	if v, _ := rt.Get(out); v != -1 {
		t.Fatalf("out=%v want -1 (timed-out result must not be written back)", v)
	}
	// 运行时仍健康：新触发正常执行。
	rt.Effect("fast", []Handle{src}, func(c *EffectContext) error {
		return c.Set(out, 1)
	})
	if err := rt.Set(src, 1); err != nil {
		t.Fatal(err)
	}
	rt.Flush()
	if v, _ := rt.Get(out); v != 1 {
		t.Fatalf("out=%v want 1 after recovery", v)
	}
}

// 取消：被取代的执行观察到 Done 关闭，其写回被丢弃。
func TestCancellation(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	src := rt.State("src", 0)
	out := rt.State("out", -1)

	started := make(chan struct{}, 4)
	var calls, sawCancel atomic.Int64
	rt.Effect("eff", []Handle{src}, func(c *EffectContext) error {
		n := calls.Add(1)
		started <- struct{}{}
		if n <= 2 {
			<-c.Done // 前两次执行等待被取消
			sawCancel.Add(1)
			_ = c.Set(out, 555) // 已取消，写回必须失败
			return errors.New("cancelled")
		}
		return nil // 第三次（最终）执行正常完成
	})
	<-started                              // 初始执行
	if err := rt.Set(src, 1); err != nil { // 触发取代
		t.Fatal(err)
	}
	<-started // 重跑的执行
	if err := rt.Set(src, 2); err != nil {
		t.Fatal(err)
	}
	rt.Flush()
	if sawCancel.Load() < 1 {
		t.Fatal("cancelled run did not observe Done")
	}
	if v, _ := rt.Get(out); v != -1 {
		t.Fatalf("out=%v want -1 (cancelled writes must be discarded)", v)
	}
}

// 副作用错误可观察，且不污染运行时。
func TestEffectErrorObservable(t *testing.T) {
	errCh := make(chan error, 1)
	rt := New(Options{})
	defer rt.Close()
	src := rt.State("src", 0)
	want := errors.New("effect failed")
	rt.EffectWithOptions("bad", []Handle{src}, EffectOptions{
		OnError: func(err error) { errCh <- err },
	}, func(c *EffectContext) error {
		if v, _ := c.Get(src); v.(int) == 1 {
			return want
		}
		return nil
	})
	rt.Flush()
	if err := rt.Set(src, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, want) {
			t.Fatalf("OnError got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnError not called")
	}
	// 运行时继续工作。
	if err := rt.Set(src, 2); err != nil {
		t.Fatal(err)
	}
	rt.Flush()
}

// 队列深度上限：超限拒绝且行为可观测。
func TestQueueOverflowReject(t *testing.T) {
	var overflows atomic.Int64
	rt := New(Options{
		MaxConcurrency: 1,
		MaxQueue:       1,
		Overflow:       Reject,
		OnOverflow:     func(name string) { overflows.Add(1) },
	})
	defer rt.Close()
	src := rt.State("src", 0)

	release := make(chan struct{})
	var started atomic.Int64
	// 多个互不相同的副作用，填满 worker 与队列。
	rt.Effect("e1", []Handle{src}, func(c *EffectContext) error {
		started.Add(1)
		<-release
		return nil
	})
	rt.Effect("e2", []Handle{src}, func(c *EffectContext) error {
		started.Add(1)
		<-release
		return nil
	})
	rt.Effect("e3", []Handle{src}, func(c *EffectContext) error {
		started.Add(1)
		<-release
		return nil
	})
	// 等待 e1 占住唯一 worker，e2 占住唯一队列位。
	for started.Load() < 1 {
		time.Sleep(time.Millisecond)
	}
	deadline := time.Now().Add(2 * time.Second)
	for overflows.Load() == 0 && time.Now().Before(deadline) {
		// 继续触发直到出现拒绝（e1 在途、e2 在队列、e3 的触发将被拒绝）。
		if err := rt.Set(src, int(started.Load()+overflows.Load())+1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	rt.Flush()
	if overflows.Load() == 0 {
		t.Fatal("expected observable overflow")
	}
}

// 慢副作用不得阻塞无关分支。
func TestSlowEffectDoesNotBlockOthers(t *testing.T) {
	rt := New(Options{MaxConcurrency: 2})
	defer rt.Close()
	a := rt.State("a", 0)
	b := rt.State("b", 0)

	slowEntered := make(chan struct{})
	fastDone := make(chan struct{})
	rt.Effect("slow", []Handle{a}, func(c *EffectContext) error {
		if v, _ := c.Get(a); v.(int) == 1 {
			close(slowEntered)
			time.Sleep(300 * time.Millisecond)
		}
		return nil
	})
	rt.Effect("fast", []Handle{b}, func(c *EffectContext) error {
		if v, _ := c.Get(b); v.(int) == 1 {
			close(fastDone)
		}
		return nil
	})
	rt.Flush()

	if err := rt.Set(a, 1); err != nil {
		t.Fatal(err)
	}
	<-slowEntered
	if err := rt.Set(b, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fastDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("fast effect blocked by slow effect")
	}
	rt.Flush()
}

// 动态增删副作用：已注销的副作用不再被调度。
func TestDynamicEffectRemoval(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	src := rt.State("src", 0)
	var runs atomic.Int64
	h := rt.Effect("eff", []Handle{src}, func(c *EffectContext) error {
		runs.Add(1)
		return nil
	})
	rt.Flush()
	if err := rt.Set(src, 1); err != nil {
		t.Fatal(err)
	}
	rt.Flush()
	before := runs.Load()
	rt.Dispose(h)
	for i := 2; i < 6; i++ {
		if err := rt.Set(src, i); err != nil {
			t.Fatal(err)
		}
	}
	rt.Flush()
	if runs.Load() != before {
		t.Fatalf("disposed effect ran: before=%d after=%d", before, runs.Load())
	}
}

// 并发提交下副作用最终状态确定。
func TestConcurrentCommitsFinalState(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		rt := New(Options{})
		src := rt.State("src", 0)
		out := rt.State("out", 0)
		rt.Effect("recorder", []Handle{src}, func(c *EffectContext) error {
			v, err := c.Get(src)
			if err != nil {
				return nil
			}
			_ = c.Set(out, v.(int))
			return nil
		})
		var wg sync.WaitGroup
		for w := 0; w < 20; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					_ = rt.Update(func(tx *Tx) error {
						v, _ := tx.Get(src)
						tx.Set(src, v.(int)+1)
						return nil
					})
				}
			}()
		}
		wg.Wait()
		rt.Flush()
		if v, _ := rt.Get(src); v != 200 {
			t.Fatalf("src=%v want 200", v)
		}
		// out 由最后一次生效的副作用写入，必须 <= src 且为某次提交的快照。
		if v, _ := rt.Get(out); v.(int) > 200 || v.(int) < 0 {
			t.Fatalf("out=%v out of range", v)
		}
		rt.Close()
	}
}

// 长期运行不出现无界增长：节点注销后引用被释放（通过节点计数间接验证）。
func TestNoUnboundedGrowth(t *testing.T) {
	rt := New(Options{})
	defer rt.Close()
	src := rt.State("src", 0)
	for i := 0; i < 1000; i++ {
		d := rt.Derive("d", func(s *Scope) (any, error) { return s.Must(src), nil })
		e := rt.Effect("e", []Handle{src}, func(c *EffectContext) error { return nil })
		rt.Dispose(d)
		rt.Dispose(e)
	}
	rt.Flush()
	rt.mu.Lock()
	n := len(rt.nodes)
	rt.mu.Unlock()
	if n != 1 { // 只剩 src
		t.Fatalf("live nodes=%d want 1 (leak)", n)
	}
}

// 调度顺序确定：同一批触发按节点创建顺序（id）FIFO 执行，与遍历顺序无关。
func TestEffectSchedulingOrderDeterministic(t *testing.T) {
	runOnce := func() []string {
		rt := New(Options{MaxConcurrency: 1}) // 单并发使执行顺序=提交顺序
		defer rt.Close()
		src := rt.State("src", 0)
		var mu sync.Mutex
		var order []string
		record := func(name string) func(*EffectContext) error {
			return func(c *EffectContext) error {
				if v, _ := c.Get(src); v.(int) == 0 {
					return nil // 跳过初始执行
				}
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				return nil
			}
		}
		names := []string{"e1", "e2", "e3", "e4", "e5"}
		handles := make([]Handle, 0, len(names))
		for _, n := range names {
			handles = append(handles, rt.Effect(n, []Handle{src}, record(n)))
		}
		_ = handles
		rt.Flush()
		if err := rt.Set(src, 1); err != nil {
			t.Fatal(err)
		}
		rt.Flush()
		return order
	}
	want := runOnce()
	if len(want) != 5 {
		t.Fatalf("order=%v", want)
	}
	for i := 0; i < 10; i++ {
		got := runOnce()
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("scheduling order not deterministic: %v vs %v", want, got)
		}
	}
	if !reflect.DeepEqual(want, []string{"e1", "e2", "e3", "e4", "e5"}) {
		t.Fatalf("unexpected order %v", want)
	}
}

// DropOldest 策略：队列满时丢弃最旧的排队任务，行为确定。
func TestQueueOverflowDropOldest(t *testing.T) {
	rt := New(Options{MaxConcurrency: 1, MaxQueue: 1, Overflow: DropOldest})
	defer rt.Close()
	src := rt.State("src", 0)

	release := make(chan struct{})
	var mu sync.Mutex
	var ran []int
	blocker := rt.State("blocker", 0)
	rt.Effect("blocker", []Handle{blocker}, func(c *EffectContext) error {
		<-release
		return nil
	})
	// blocker 的初始执行占住唯一 worker 并阻塞在 release 上。
	// 再次触发使其合并为 pending（单在途语义）。
	if err := rt.Set(blocker, 1); err != nil {
		t.Fatal(err)
	}
	// 再注册两个副作用，它们的初始执行将竞争唯一队列位：
	// e1 入队，e2 入队时挤掉 e1（DropOldest）。
	rt.Effect("e1", []Handle{src}, func(c *EffectContext) error {
		mu.Lock()
		ran = append(ran, 1)
		mu.Unlock()
		return nil
	})
	rt.Effect("e2", []Handle{src}, func(c *EffectContext) error {
		mu.Lock()
		ran = append(ran, 2)
		mu.Unlock()
		return nil
	})
	close(release)
	rt.Flush()
	mu.Lock()
	defer mu.Unlock()
	if len(ran) != 1 || ran[0] != 2 {
		t.Fatalf("ran=%v, want [2] (e1 dropped as oldest)", ran)
	}
}
