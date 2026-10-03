package reactive

import "context"

// Effect is an asynchronous side-effect leaf triggered by dependency changes.
type Effect struct {
	n *node
}

// NewEffect registers a side effect and schedules it once with current values.
func NewEffect(rt *Runtime, fn func(context.Context, Values) error, deps ...Node) *Effect {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil
	}

	n := rt.newNode(KindEffect)
	n.effectFn = fn
	for _, dep := range deps {
		if dep == nil {
			continue
		}
		dn := dep.node()
		if dn == nil || dn.retired || dn.rt != rt {
			continue
		}
		addEdge(dn, n)
	}

	values := snapshotValues(n)
	_, err := failedDependency(n)
	rt.mu.Unlock()

	if err != nil {
		rt.emitEffectSkipped(n, err)
	} else {
		rt.sched.enqueue(n, rt.version, values)
	}
	return &Effect{n: n}
}

// ID returns the stable node identifier.
func (e *Effect) ID() ID { return e.n.id }

func (e *Effect) node() *node { return e.n }
