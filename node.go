package reactive

import "errors"

// Node is implemented by State, Derived, and Effect handles.
type Node interface {
	ID() ID
	node() *node
}

func resolveNode(n Node) (*node, error) {
	if n == nil {
		return nil, errors.New("reactive: nil node")
	}
	x := n.node()
	if x == nil || x.rt == nil || x.retired || x.rt.closed {
		return nil, errors.New("reactive: node is not available")
	}
	return x, nil
}

// AddDependency adds a dependency edge to a derived computation or effect.
func AddDependency(n, dep Node) error {
	rt := runtimeOf(n)
	if rt == nil {
		return errors.New("reactive: nil runtime")
	}
	rt.mu.Lock()
	var after func()
	defer func() {
		if after != nil {
			rt.orderMu.Lock()
		}
		rt.mu.Unlock()
		if after != nil {
			defer rt.orderMu.Unlock()
			after()
		}
	}()

	target, err := resolveNode(n)
	if err != nil {
		return err
	}
	from, err := resolveNode(dep)
	if err != nil {
		return err
	}
	if target.kind != KindDerived && target.kind != KindEffect {
		return errors.New("reactive: dependency owner must be derived or effect")
	}
	if from.kind != KindState && from.kind != KindDerived {
		return errors.New("reactive: dependency must be state or derived")
	}
	if target == from {
		return errors.New("reactive: dependency cycle")
	}
	for _, existing := range target.deps {
		if existing == from {
			return nil
		}
	}
	if reaches(from, target) {
		return errors.New("reactive: dependency cycle")
	}

	addEdge(from, target)
	rt.version++
	version := rt.version

	var ordered []*node
	if target.kind == KindDerived {
		downstream, affected := collectAffectedOrdered([]*node{target})
		affected[target] = true
		ordered = append([]*node{target}, downstream...)
		done := make(map[*node]bool, len(ordered))
		for _, candidate := range ordered {
			evaluateAffected(candidate, affected, done)
		}
	} else {
		ordered = []*node{target}
	}
	scheduled := prepareOrderedEffects(ordered, version)
	after = func() {
		enqueueScheduledEffects(rt, scheduled)
	}
	return nil
}

// RemoveDependency removes a dependency edge from a derived computation or effect.
func RemoveDependency(n, dep Node) error {
	rt := runtimeOf(n)
	if rt == nil {
		return errors.New("reactive: nil runtime")
	}
	rt.mu.Lock()
	var after func()
	defer func() {
		if after != nil {
			rt.orderMu.Lock()
		}
		rt.mu.Unlock()
		if after != nil {
			defer rt.orderMu.Unlock()
			after()
		}
	}()

	target, err := resolveNode(n)
	if err != nil {
		return err
	}
	from, err := resolveNode(dep)
	if err != nil {
		return err
	}
	if !removeEdge(from, target) {
		return errors.New("reactive: dependency edge does not exist")
	}

	rt.version++
	version := rt.version
	ordered := []*node{target}
	if target.kind == KindDerived {
		downstream, affected := collectAffectedOrdered([]*node{target})
		affected[target] = true
		ordered = append(ordered, downstream...)
		done := make(map[*node]bool, len(ordered))
		for _, candidate := range ordered {
			evaluateAffected(candidate, affected, done)
		}
	}
	scheduled := prepareOrderedEffects(ordered, version)
	after = func() {
		enqueueScheduledEffects(rt, scheduled)
	}
	return nil
}

// Remove detaches a node and cancels effects owned by the runtime.
func Remove(n Node) error {
	rt := runtimeOf(n)
	if rt == nil {
		return errors.New("reactive: nil runtime")
	}
	rt.mu.Lock()
	var after func()
	defer func() {
		if after != nil {
			rt.orderMu.Lock()
		}
		rt.mu.Unlock()
		if after != nil {
			defer rt.orderMu.Unlock()
			after()
		}
	}()

	target, err := resolveNode(n)
	if err != nil {
		return err
	}
	if target.kind == KindState && len(target.dependents) > 0 {
		return errors.New("reactive: state has dependents")
	}

	dependents := append([]*node(nil), target.dependents...)
	deps := append([]*node(nil), target.deps...)
	for _, dep := range deps {
		removeEdge(dep, target)
	}
	for _, dependent := range dependents {
		removeEdge(target, dependent)
	}

	if target.kind == KindEffect {
		target.sched.mu.Lock()
		target.sched.retired = true
		if target.sched.cancel != nil {
			target.sched.cancel()
		}
		target.sched.mu.Unlock()
	}

	target.retired = true
	delete(rt.nodes, target.id)
	rt.version++
	version := rt.version

	var ordered []*node
	if target.kind != KindEffect {
		downstream, affected := collectAffectedOrdered(dependents)
		ordered = append([]*node(nil), dependents...)
		for _, candidate := range downstream {
			if !affected[candidate] {
				affected[candidate] = true
				ordered = append(ordered, candidate)
			}
		}
		done := make(map[*node]bool, len(ordered))
		for _, candidate := range ordered {
			evaluateAffected(candidate, affected, done)
		}
	}

	if target.kind != KindEffect {
		scheduled := prepareOrderedEffects(ordered, version)
		after = func() {
			enqueueScheduledEffects(rt, scheduled)
		}
	}
	return nil
}

func runtimeOf(n Node) *Runtime {
	if n == nil {
		return nil
	}
	x := n.node()
	if x == nil {
		return nil
	}
	return x.rt
}
