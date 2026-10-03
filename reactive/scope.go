package reactive

// Scope 是派生计算求值期间的读取上下文。
// 通过它读取依赖会动态登记依赖边：本次求值读到的节点集合
// 即为本节点的新依赖集，未再读取的旧依赖会被自动摘除。
// Scope 仅在求值期间有效，求值由运行时串行执行（持有 r.mu）。
type Scope struct {
	rt   *Runtime
	self *node
	deps map[*node]struct{}
}

// Get 读取一个节点的当前值；若节点处于错误态，返回其错误。
// 已注销的节点返回 ErrDisposed。
func (s *Scope) Get(h Handle) (any, error) {
	n := h.n
	if n.disposed {
		return nil, ErrDisposed
	}
	s.deps[n] = struct{}{}
	return n.value, n.err
}

// Must 读取节点值，出错时 panic；运行时会捕获并转换为本节点的错误态。
func (s *Scope) Must(h Handle) any {
	v, err := s.Get(h)
	if err != nil {
		panic(err)
	}
	return v
}

// EffectContext 是副作用执行时的上下文。
// 副作用在调度器 goroutine 中异步执行，读取与写回均经过运行时串行化。
type EffectContext struct {
	// Done 在本次执行被取代（新触发）、超时或运行时关闭时关闭。
	Done <-chan struct{}
	// Gen 本次执行的代次。
	Gen uint64

	rt   *Runtime
	self *node
}

// Get 读取依赖节点当前已提交的值。
func (c *EffectContext) Get(h Handle) (any, error) {
	return c.rt.Get(h)
}

// Set 把结果写回某个状态节点。
// 仅当本次执行仍是最新代次（未被取代、未超时、未注销）时才生效；
// 否则返回 ErrDisposed，过期结果一律被丢弃。
func (c *EffectContext) Set(h Handle, v any) error {
	return c.rt.effectSet(c.self, c.Gen, h, v)
}
