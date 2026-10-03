package reactive

import (
	"fmt"
	"sort"
	"sync"
)

// Handle 是节点的引用句柄。节点 id 按创建顺序单调递增，
// 传播与调度顺序均以 id 为tie-breaker，保证确定性。
type Handle struct {
	n *node
}

// Name 返回节点名。
func (h Handle) Name() string { return h.n.name }

// Runtime 是响应式运行时：持有依赖图、提交事务并调度传播与副作用。
//
// 并发模型：所有图结构变更与值提交都在 r.mu 下串行完成，
// 一次提交要么全部生效要么完全不生效；读取方只能看到某个已提交的完整版本。
type Runtime struct {
	mu      sync.Mutex
	opts    Options
	nextID  uint64
	version uint64 // 已提交版本号
	closed  bool

	nodes map[uint64]*node

	sched *scheduler

	notifs []func() // 待投递的锁外回调（溢出通知等）
}

// New 创建运行时。
func New(opts Options) *Runtime {
	o := opts.withDefaults()
	return &Runtime{
		opts:  o,
		nodes: make(map[uint64]*node),
		sched: newScheduler(o.MaxConcurrency, o.MaxQueue, o.Overflow),
	}
}

// State 登记一个可写状态节点。
func (r *Runtime) State(name string, initial any) Handle {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.addNode(kindSource, name)
	n.value = initial
	n.version = r.version
	return Handle{n}
}

// Derive 登记一个派生计算节点，并立即完成首次求值。
func (r *Runtime) Derive(name string, compute func(*Scope) (any, error)) Handle {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.addNode(kindDerived, name)
	n.compute = compute
	r.evalDerived(n)
	return Handle{n}
}

// Effect 登记一个副作用节点。deps 中任一节点在一次提交中发生变化时，
// 该副作用被触发一次（异步执行，同一副作用只允许一个在途执行）。
// 登记时会触发一次初始执行。
func (r *Runtime) Effect(name string, deps []Handle, run func(*EffectContext) error) Handle {
	return r.EffectWithOptions(name, deps, EffectOptions{}, run)
}

// EffectWithOptions 带配置的副作用登记。
func (r *Runtime) EffectWithOptions(name string, deps []Handle, eo EffectOptions, run func(*EffectContext) error) Handle {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.addNode(kindEffect, name)
	n.effect = &effectSpec{run: run, onError: eo.OnError, timeout: eo.Timeout}
	for _, h := range deps {
		r.addEdge(h.n, n)
	}
	r.refreshLevel(n)
	r.triggerEffectLocked(n)
	notifs := r.drainNotifsLocked()
	r.mu.Unlock()
	for _, f := range notifs {
		f()
	}
	r.mu.Lock()
	return Handle{n}
}

func (r *Runtime) addNode(kind nodeKind, name string) *node {
	r.nextID++
	n := &node{
		id:   r.nextID,
		kind: kind,
		name: name,
		deps: make(map[*node]struct{}),
		subs: make(map[*node]struct{}),
	}
	r.nodes[n.id] = n
	return n
}

func (r *Runtime) addEdge(from, to *node) {
	from.subs[to] = struct{}{}
	to.deps[from] = struct{}{}
}

func (r *Runtime) removeEdge(from, to *node) {
	delete(from.subs, to)
	delete(to.deps, from)
}

// refreshLevel 重算节点拓扑层级（仅用于观测与调试，调度不依赖它）。
func (r *Runtime) refreshLevel(n *node) {
	lvl := 0
	for d := range n.deps {
		if d.level+1 > lvl {
			lvl = d.level + 1
		}
	}
	n.level = lvl
}

// Get 读取节点当前已提交的值与错误态。
func (r *Runtime) Get(h Handle) (any, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h.n.disposed {
		return nil, ErrDisposed
	}
	return h.n.value, h.n.err
}

// Set 设置单个状态节点的值（等价于单操作事务）。
func (r *Runtime) Set(h Handle, v any) error {
	return r.Update(func(tx *Tx) error {
		tx.Set(h, v)
		return nil
	})
}

// Update 在一个事务中执行多次状态变更：
// fn 在运行时锁内执行（可串行化），应只使用 tx.Get/tx.Set，
// 不得调用 Runtime 的其它方法（会死锁）。
// fn 返回错误（或 panic）时所有变更被丢弃，状态不停留在中间态；
// fn 成功时全部变更原子生效并触发一次传播。
func (r *Runtime) Update(fn func(tx *Tx) error) (err error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrClosed
	}
	tx := &Tx{rt: r, sets: make(map[*node]any)}
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("reactive: transaction panicked: %v", p)
			}
		}()
		err = fn(tx)
	}()
	if err != nil {
		r.mu.Unlock()
		return err
	}
	if tx.err != nil {
		err = tx.err
		r.mu.Unlock()
		return err
	}
	for n := range tx.sets {
		if n.disposed {
			r.mu.Unlock()
			return ErrDisposed
		}
	}
	r.commitLocked(tx.sets)
	notifs := r.drainNotifsLocked()
	r.mu.Unlock()
	for _, f := range notifs {
		f()
	}
	return nil
}

// drainNotifsLocked 取出并清空待投递回调。调用方须持有 r.mu。
func (r *Runtime) drainNotifsLocked() []func() {
	if len(r.notifs) == 0 {
		return nil
	}
	fs := r.notifs
	r.notifs = nil
	return fs
}

// commitLocked 原子应用一批状态变更并同步传播。调用方须持有 r.mu。
func (r *Runtime) commitLocked(sets map[*node]any) {
	if len(sets) == 0 {
		return
	}
	r.version++
	for n, v := range sets {
		n.value = v
		n.err = nil
		n.version = r.version
	}

	// 1. 从变更源沿 subs 标记受影响的派生节点与副作用。
	dirty := make(map[*node]struct{})
	effects := make(map[*node]struct{})
	queue := make([]*node, 0, len(sets))
	for n := range sets {
		queue = append(queue, n)
	}
	for len(queue) > 0 {
		cur := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		for sub := range cur.subs {
			switch sub.kind {
			case kindDerived:
				if _, ok := dirty[sub]; !ok {
					dirty[sub] = struct{}{}
					queue = append(queue, sub)
				}
			case kindEffect:
				effects[sub] = struct{}{}
			}
		}
	}

	// 2. 按拓扑序（Kahn：每次取依赖已全部就绪的最小 id）求值，
	//    每个派生节点在本批中只求值一次，且总是基于本批最新值。
	for len(dirty) > 0 {
		var ready *node
		for n := range dirty {
			depsReady := true
			for d := range n.deps {
				if _, ok := dirty[d]; ok {
					depsReady = false
					break
				}
			}
			if !depsReady {
				continue
			}
			if ready == nil || n.id < ready.id {
				ready = n
			}
		}
		if ready == nil { // 存在环，无法继续；剩余节点标记为错误
			for n := range dirty {
				n.err = fmt.Errorf("reactive: dependency cycle involving %q", n.name)
				n.version = r.version
				delete(dirty, n)
			}
			break
		}
		delete(dirty, ready)
		r.evalDerived(ready)
	}

	// 3. 触发副作用：按 id 排序保证调度顺序确定。
	if len(effects) > 0 {
		list := make([]*node, 0, len(effects))
		for n := range effects {
			list = append(list, n)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].id < list[j].id })
		for _, n := range list {
			r.triggerEffectLocked(n)
		}
	}
}

// evalDerived 对派生节点求值一次并更新动态依赖边。调用方须持有 r.mu。
func (r *Runtime) evalDerived(n *node) {
	s := &Scope{rt: r, self: n, deps: make(map[*node]struct{})}
	v, err := func() (v any, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("reactive: derived %q panicked: %v", n.name, p)
			}
		}()
		return n.compute(s)
	}()
	if err != nil {
		// 失败策略：保留最近一次成功值，记录错误；依赖它的节点读到错误。
		n.err = err
	} else {
		n.value = v
		n.err = nil
	}
	n.version = r.version

	// 更新动态依赖边。
	for d := range n.deps {
		if _, ok := s.deps[d]; !ok {
			r.removeEdge(d, n)
		}
	}
	for d := range s.deps {
		if _, ok := n.deps[d]; !ok {
			r.addEdge(d, n)
		}
	}
	r.refreshLevel(n)
}

// Dispose 注销节点：移除全部依赖边，取消在途副作用，
// 之后该节点不再被调度，运行时不再持有其引用。
func (r *Runtime) Dispose(h Handle) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := h.n
	if n.disposed {
		return
	}
	n.disposed = true
	for d := range n.deps {
		delete(d.subs, n)
	}
	n.deps = make(map[*node]struct{})
	// 让订阅者重新求值，以便它们动态摘除对已注销节点的依赖。
	subs := make([]*node, 0, len(n.subs))
	for s := range n.subs {
		subs = append(subs, s)
	}
	n.subs = make(map[*node]struct{})
	delete(r.nodes, n.id)
	if n.kind == kindEffect && n.effect != nil && n.effect.inflight {
		n.effect.gen++ // 使在途结果过期
		if n.effect.done != nil {
			close(n.effect.done)
			n.effect.done = nil
		}
	}
	// 对仍引用本节点的派生订阅者触发一次重算（它们会读到 ErrDisposed）。
	for _, s := range subs {
		if s.kind == kindDerived && !s.disposed {
			r.evalDerived(s)
		}
	}
}

// Flush 等待当前所有已触发的副作用执行完毕。
func (r *Runtime) Flush() { r.sched.waitIdle() }

// Close 关闭运行时：取消在途副作用并等待其退出，拒绝后续提交。
func (r *Runtime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	for _, n := range r.nodes {
		if n.kind == kindEffect && n.effect != nil && n.effect.inflight {
			n.effect.gen++
			if n.effect.done != nil {
				close(n.effect.done)
				n.effect.done = nil
			}
		}
	}
	r.mu.Unlock()
	for _, t := range r.sched.close() {
		if t.onDrop == nil {
			continue
		}
		r.mu.Lock()
		t.onDrop()
		r.mu.Unlock()
	}
}

// Tx 是一次事务。仅可记录对可写状态节点的修改。
type Tx struct {
	rt   *Runtime
	sets map[*node]any
	err  error
}

// Get 读取节点在事务开始时的已提交值（含本事务内已覆盖的新值）。
// 仅在 Update 的 fn 内调用。
func (t *Tx) Get(h Handle) (any, error) {
	n := h.n
	if n.disposed {
		return nil, ErrDisposed
	}
	if v, ok := t.sets[n]; ok {
		return v, nil
	}
	return n.value, n.err
}

// Set 在事务中设置状态节点的值。
func (t *Tx) Set(h Handle, v any) {
	if t.err != nil {
		return
	}
	n := h.n
	if n.disposed {
		t.err = ErrDisposed
		return
	}
	if n.kind != kindSource {
		t.err = fmt.Errorf("reactive: cannot set %q: not a state node", n.name)
		return
	}
	t.sets[n] = v
}
