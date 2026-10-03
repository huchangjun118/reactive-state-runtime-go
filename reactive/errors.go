package reactive

import "errors"

// ErrQueueFull 表示副作用队列已满且策略为拒绝。
var ErrQueueFull = errors.New("reactive: effect queue full")

// ErrDisposed 表示对已注销节点的操作。
var ErrDisposed = errors.New("reactive: node disposed")

// ErrClosed 表示运行时已关闭。
var ErrClosed = errors.New("reactive: runtime closed")

// DependencyError 表示派生计算因上游依赖失败而失败。
type DependencyError struct {
	Node string
	Err  error
}

func (e *DependencyError) Error() string {
	return "reactive: dependency " + e.Node + " failed: " + e.Err.Error()
}
func (e *DependencyError) Unwrap() error { return e.Err }
