package reactive

import "errors"

// Tx contains state changes belonging to one atomic transaction.
type Tx struct {
	rt     *Runtime
	staged map[*node]any
	order  []*node
}

// Update runs fn as one committed transaction. If fn returns an error, no state
// or derived result is published.
func (rt *Runtime) Update(fn func(*Tx) error) (version uint64, err error) {
	if fn == nil {
		return rt.currentVersion(), nil
	}

	rt.mu.Lock()
	locked := true
	var after func()
	defer func() {
		if after != nil {
			rt.orderMu.Lock()
		}
		if locked {
			rt.mu.Unlock()
		}
		if after != nil {
			defer rt.orderMu.Unlock()
			after()
		}
	}()
	if rt.closed {
		return 0, errors.New("reactive runtime is closed")
	}

	tx := &Tx{rt: rt, staged: make(map[*node]any), order: make([]*node, 0)}
	if err = fn(tx); err != nil {
		return 0, err
	}
	if len(tx.staged) == 0 {
		return rt.version, nil
	}

	changed := tx.order
	oldValues := make(map[*node]any, len(changed))
	oldErrors := make(map[*node]error, len(changed))
	for _, n := range changed {
		value := tx.staged[n]
		oldValues[n] = n.value
		oldErrors[n] = n.err
		n.value = value
		n.err = nil
	}

	affectedList, affected := collectAffectedOrdered(changed)
	for n := range affected {
		if n.kind == KindDerived {
			oldValues[n] = n.value
			oldErrors[n] = n.err
		}
	}

	done := make(map[*node]bool, len(affected))
	for n := range affected {
		evaluateAffected(n, affected, done)
		if rt.config.ErrorPolicy == ErrorPolicyFailFast && n.kind == KindDerived && n.err != nil {
			err = n.err
			for oldNode, oldValue := range oldValues {
				oldNode.value = oldValue
				oldNode.err = oldErrors[oldNode]
			}
			rt.metrics.TransactionFailures.Add(1)
			rt.emitTransaction(0, n.err)
			return 0, err
		}
	}

	rt.version++
	version = rt.version
	rt.metrics.Transactions.Add(1)
	for n := range affected {
		if n.kind == KindDerived {
			n.computedAt = version
		}
	}
	scheduled := prepareOrderedEffects(affectedList, version)
	after = func() {
		enqueueScheduledEffects(rt, scheduled)
		rt.emitTransaction(version, nil)
	}
	return version, nil
}

func (tx *Tx) set(n *node, value any) {
	if n == nil || n.kind != KindState {
		return
	}
	if _, exists := tx.staged[n]; !exists {
		tx.order = append(tx.order, n)
	}
	tx.staged[n] = value
}
