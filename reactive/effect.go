package reactive

import "time"

// EffectOptions 副作用配置。
type EffectOptions struct {
	// Timeout 单次执行超时；超时后本次执行被取消（Done 关闭），
	// 其结果与写回一律被丢弃。<=0 表示不超时。
	Timeout time.Duration
	// OnError 本次执行（未被取代）返回错误时的回调。
	OnError func(err error)
}

// triggerEffectLocked 触发一次副作用。调用方须持有 r.mu。
// 同一副作用只允许一个在途执行：在途期间的新触发会取消当前执行
// 并记录 pending，待当前执行退出后以最新代次重跑。
func (r *Runtime) triggerEffectLocked(n *node) {
	es := n.effect
	es.gen++
	if es.inflight {
		es.pending = true
		if es.done != nil {
			close(es.done) // 取消在途执行，其结果将被丢弃
			es.done = nil
		}
		return
	}
	r.launchEffectLocked(n)
}

// launchEffectLocked 启动一次副作用执行。调用方须持有 r.mu。
func (r *Runtime) launchEffectLocked(n *node) {
	es := n.effect
	es.inflight = true
	es.done = make(chan struct{})
	gen := es.gen
	done := es.done

	var timer *time.Timer
	if es.timeout > 0 {
		timer = time.AfterFunc(es.timeout, func() {
			r.mu.Lock()
			// 仅当在途的仍是本次执行时才按超时取消。
			if es.inflight && es.gen == gen && es.done == done {
				es.gen++ // 使本次结果过期
				close(done)
				es.done = nil
			}
			r.mu.Unlock()
		})
	}

	dropped, submitErr := r.sched.submit(&task{
		run: func() {
			if timer != nil {
				defer timer.Stop()
			}
			r.runEffect(n, gen, done)
		},
		// onDrop 在持有 r.mu 的上下文中被调用。
		onDrop: func() {
			if timer != nil {
				timer.Stop()
			}
			if es.gen == gen {
				es.inflight = false
			}
		},
	})
	if dropped != nil && dropped.onDrop != nil {
		dropped.onDrop() // 持有 r.mu
	}
	if submitErr != nil {
		// 队列满或已关闭：本次触发被丢弃（可观测），状态保持一致。
		if timer != nil {
			timer.Stop()
		}
		es.inflight = false
		es.done = nil
		if cb := r.opts.OnOverflow; cb != nil && submitErr == ErrQueueFull {
			name := n.name
			r.notifs = append(r.notifs, func() { cb(name) })
		}
	}
}

// runEffect 在调度器 goroutine 中执行副作用。
func (r *Runtime) runEffect(n *node, gen uint64, done chan struct{}) {
	es := n.effect
	ctx := &EffectContext{Done: done, Gen: gen, rt: r, self: n}
	runErr := es.run(ctx)

	r.mu.Lock()
	stale := es.gen != gen || n.disposed
	// 单在途：在途执行就是本次，完成时一律清理。
	es.inflight = false
	es.done = nil
	relaunch := es.pending && !n.disposed && !r.closed
	if relaunch {
		es.pending = false
	}
	var errCb func(error)
	var runtimeErrCb func(string, error)
	if !stale && runErr != nil {
		errCb = es.onError
		runtimeErrCb = r.opts.OnEffectError
	}
	name := n.name
	if relaunch {
		r.launchEffectLocked(n)
	}
	r.mu.Unlock()

	// 回调在锁外执行，避免用户代码重入死锁。
	if errCb != nil {
		errCb(runErr)
	}
	if runtimeErrCb != nil {
		runtimeErrCb(name, runErr)
	}
}

// effectSet 副作用通过 EffectContext.Set 写回状态：
// 仅当本次执行未被取代（gen 一致）且节点未注销时才生效，
// 过期、取消、超时后的写回一律被丢弃。
func (r *Runtime) effectSet(n *node, gen uint64, h Handle, v any) (err error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	if n.disposed || n.effect.gen != gen {
		r.mu.Unlock()
		return ErrDisposed // 过期结果，丢弃
	}
	if h.n.disposed {
		r.mu.Unlock()
		return ErrDisposed
	}
	if h.n.kind != kindSource {
		name := h.n.name
		r.mu.Unlock()
		return &DependencyError{Node: name, Err: ErrDisposed}
	}
	r.commitLocked(map[*node]any{h.n: v})
	notifs := r.drainNotifsLocked()
	r.mu.Unlock()
	for _, f := range notifs {
		f()
	}
	return nil
}
