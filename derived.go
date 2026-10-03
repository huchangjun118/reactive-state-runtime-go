package reactive

import "context"

// Derived is a lazily evaluated value computed from other graph nodes.
type Derived[T any] struct {
	n *node
}

// NewDerived registers a derived computation. Dependencies are evaluated in
// their declared order and each affected computation runs at most once per batch.
func NewDerived[T any](rt *Runtime, fn func(context.Context, Values) (T, error), deps ...Node) *Derived[T] {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return nil
	}

	n := rt.newNode(KindDerived)
	n.derivedFn = func(_ context.Context, values Values) (any, error) {
		return fn(context.Background(), values)
	}
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

	evaluateAffected(n, map[*node]bool{n: true}, map[*node]bool{})
	return &Derived[T]{n: n}
}

// Get returns the latest computed value, version, and error.
func (d *Derived[T]) Get() (T, uint64, error) {
	d.n.rt.mu.Lock()
	defer d.n.rt.mu.Unlock()
	value, _ := d.n.value.(T)
	return value, d.n.rt.version, d.n.err
}

// ID returns the stable node identifier.
func (d *Derived[T]) ID() ID { return d.n.id }

func (d *Derived[T]) node() *node { return d.n }
