package reactive

// OverflowPolicy 队列满时的处理策略。
type OverflowPolicy int

const (
	// Reject 拒绝新任务并返回 ErrQueueFull。
	Reject OverflowPolicy = iota
	// DropOldest 丢弃队列中最旧的任务（该次触发被合并丢失，行为确定）。
	DropOldest
)

// Options 运行时配置。
type Options struct {
	// MaxConcurrency 副作用最大并发执行数，<=0 表示默认值 4。
	MaxConcurrency int
	// MaxQueue 副作用等待队列深度上限，<=0 表示默认值 64。
	MaxQueue int
	// Overflow 队列满时的策略，默认 Reject。
	Overflow OverflowPolicy
	// OnEffectError 副作用最终失败（含过期丢弃以外的错误）时的回调，可为 nil。
	OnEffectError func(effect string, err error)
	// OnOverflow 任务被拒绝/丢弃时的回调，可为 nil。
	OnOverflow func(effect string)
}

func (o *Options) withDefaults() Options {
	c := *o
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 4
	}
	if c.MaxQueue <= 0 {
		c.MaxQueue = 64
	}
	return c
}
