package reactive

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ID is the stable identity of a state, derived computation, or effect.
type ID uint64

// NodeKind describes the role of a graph node.
type NodeKind uint8

const (
	KindState NodeKind = iota
	KindDerived
	KindEffect
)

// ErrorPolicy controls how derived computation failures affect a transaction.
type ErrorPolicy uint8

const (
	// ErrorPolicyIsolate commits valid branches and records failed branches.
	ErrorPolicyIsolate ErrorPolicy = iota
	// ErrorPolicyFailFast rolls the complete transaction back on the first failure.
	ErrorPolicyFailFast
)

// QueuePolicy controls effect scheduling when the bounded queue is full.
type QueuePolicy uint8

const (
	// QueuePolicyReject drops the effect run and records an observable metric.
	QueuePolicyReject QueuePolicy = iota
	// QueuePolicyBlock blocks scheduling until queue capacity is available.
	QueuePolicyBlock
)

// Config contains runtime-wide scheduling and consistency settings.
type Config struct {
	MaxConcurrentEffects int
	MaxEffectQueue       int
	QueuePolicy          QueuePolicy
	ErrorPolicy          ErrorPolicy
	EffectTimeout        time.Duration
}

// Observer receives best-effort notifications about runtime events.
type Observer struct {
	OnTransaction   func(version uint64, err error)
	OnDerivedError  func(id ID, version uint64, err error)
	OnEffectError   func(id ID, version uint64, err error)
	OnEffectSkipped func(id ID, version uint64, err error)
	OnQueueDrop     func(id ID, version uint64)
}

// Metrics contains monotonically increasing runtime counters.
type Metrics struct {
	Transactions        atomic.Uint64
	TransactionFailures atomic.Uint64
	DerivedErrors       atomic.Uint64
	EffectRuns          atomic.Uint64
	EffectErrors        atomic.Uint64
	EffectDrops         atomic.Uint64
	EffectSkipped       atomic.Uint64
}

// Runtime is the owner of the dependency graph and asynchronous effect scheduler.
type Runtime struct {
	mu       sync.Mutex
	orderMu  sync.Mutex
	closed   bool
	nextID   ID
	nodes    map[ID]*node
	version  uint64
	config   Config
	observer Observer
	metrics  Metrics
	sched    *scheduler
}

// Values are the dependency values supplied to a derived computation or effect.
// The slice is owned by the runtime and must not be retained by user code.
type Values []any

type node struct {
	id         ID
	kind       NodeKind
	rt         *Runtime
	deps       []*node
	dependents []*node
	value      any
	err        error
	derivedFn  func(context.Context, Values) (any, error)
	effectFn   func(context.Context, Values) error
	computedAt uint64
	sched      *effectState
	retired    bool
}

type effectState struct {
	mu            sync.Mutex
	runID         uint64
	running       bool
	queued        bool
	dirty         bool
	latestVersion uint64
	latestValues  Values
	cancel        context.CancelFunc
	retired       bool
}

type task struct {
	effect  *node
	runID   uint64
	version uint64
	values  Values
}

type scheduler struct {
	rt      *Runtime
	mu      sync.Mutex
	queue   chan task
	done    chan struct{}
	wg      sync.WaitGroup
	closing bool
}

// NewRuntime creates a runtime with default configuration.
func NewRuntime(cfg Config) *Runtime {
	if cfg.MaxConcurrentEffects <= 0 {
		cfg.MaxConcurrentEffects = 4
	}
	if cfg.MaxEffectQueue <= 0 {
		cfg.MaxEffectQueue = 1024
	}
	rt := &Runtime{
		nextID: 1,
		nodes:  make(map[ID]*node),
		config: cfg,
	}
	rt.sched = newScheduler(rt)
	return rt
}

// Close cancels pending effects and waits for running effects to finish.
func (rt *Runtime) Close() error {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil
	}
	rt.closed = true
	for _, n := range rt.nodes {
		if n.sched != nil {
			n.sched.mu.Lock()
			n.sched.retired = true
			if n.sched.cancel != nil {
				n.sched.cancel()
			}
			n.sched.mu.Unlock()
		}
	}
	rt.mu.Unlock()
	rt.sched.stop()
	return nil
}

func (rt *Runtime) newNode(kind NodeKind) *node {
	n := &node{
		id:         rt.nextID,
		kind:       kind,
		rt:         rt,
		computedAt: rt.version,
	}
	rt.nextID++
	rt.nodes[n.id] = n
	if kind == KindEffect {
		n.sched = &effectState{}
	}
	return n
}

func (rt *Runtime) nodeByID(id ID) *node {
	return rt.nodes[id]
}

func (rt *Runtime) currentVersion() uint64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.version
}

func (rt *Runtime) emitTransaction(version uint64, err error) {
	if rt.observer.OnTransaction != nil {
		rt.observer.OnTransaction(version, err)
	}
}

func (rt *Runtime) emitDerivedError(n *node, err error) {
	rt.metrics.DerivedErrors.Add(1)
	if rt.observer.OnDerivedError != nil {
		rt.observer.OnDerivedError(n.id, rt.version, err)
	}
}

func (rt *Runtime) emitEffectSkipped(n *node, err error) {
	rt.metrics.EffectSkipped.Add(1)
	if rt.observer.OnEffectSkipped != nil {
		rt.observer.OnEffectSkipped(n.id, rt.version, err)
	}
}
