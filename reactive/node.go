// Package reactive 提供响应式状态与副作用调度运行时。
package reactive

import "time"

// nodeKind 标识节点类型。
type nodeKind int

const (
	kindSource  nodeKind = iota // 可写状态
	kindDerived                 // 派生计算
	kindEffect                  // 副作用
)

// node 是依赖图中的一个节点。
type node struct {
	id    uint64
	kind  nodeKind
	name  string
	level int // 拓扑层级：源为 0，派生为 max(deps)+1

	value   any
	err     error
	version uint64 // 值最近一次生效的提交版本

	// 依赖关系：deps 为本节点读取的节点，subs 为依赖本节点的节点。
	deps map[*node]struct{}
	subs map[*node]struct{}

	// 派生计算函数。
	compute func(*Scope) (any, error)

	// 副作用配置。
	effect *effectSpec

	disposed bool // 已注销
}

// effectSpec 描述一个副作用节点。
type effectSpec struct {
	run      func(ctx *EffectContext) error
	onError  func(error)
	timeout  time.Duration // 单次执行超时，<=0 表示不超时
	inflight bool          // 是否有在途执行
	pending  bool          // 在途期间又被触发，完成后需重跑
	gen      uint64        // 触发代次，用于丢弃过期结果
	done     chan struct{} // 当前在途执行的取消信号
}
