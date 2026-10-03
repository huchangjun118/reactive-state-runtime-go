package reactive

import "context"

func addEdge(dep, n *node) {
	dep.dependents = append(dep.dependents, n)
	n.deps = append(n.deps, dep)
}

func removeEdge(dep, n *node) bool {
	di := -1
	for i, candidate := range dep.dependents {
		if candidate == n {
			di = i
			break
		}
	}
	if di < 0 {
		return false
	}
	dep.dependents = append(dep.dependents[:di], dep.dependents[di+1:]...)

	ni := -1
	for i, candidate := range n.deps {
		if candidate == dep {
			ni = i
			break
		}
	}
	if ni >= 0 {
		n.deps = append(n.deps[:ni], n.deps[ni+1:]...)
	}
	return true
}

func snapshotValues(n *node) Values {
	values := make(Values, len(n.deps))
	for i, dep := range n.deps {
		values[i] = dep.value
	}
	return values
}

func failedDependency(n *node) (ID, error) {
	for _, dep := range n.deps {
		if dep.kind == KindDerived && dep.err != nil {
			return dep.id, dep.err
		}
	}
	return 0, nil
}

func evaluateAffected(n *node, affected map[*node]bool, done map[*node]bool) {
	if n == nil || n.kind != KindDerived || done[n] {
		return
	}
	done[n] = true

	for _, dep := range n.deps {
		if dep.kind == KindDerived && affected[dep] {
			evaluateAffected(dep, affected, done)
		}
	}

	values := make(Values, len(n.deps))
	for i, dep := range n.deps {
		values[i] = dep.value
	}

	if _, err := failedDependency(n); err != nil {
		n.err = err
		n.rt.emitDerivedError(n, err)
		return
	}

	value, err := safeDerived(n.derivedFn, context.Background(), values)
	if err != nil {
		n.err = err
		n.rt.emitDerivedError(n, err)
		return
	}
	n.value = value
	n.err = nil
}

func safeDerived(fn func(context.Context, Values) (any, error), ctx context.Context, values Values) (value any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError{value: r}
		}
	}()
	if fn == nil {
		return nil, nil
	}
	return fn(ctx, values)
}

type scheduledEffect struct {
	n       *node
	version uint64
	values  Values
}

func prepareOrderedEffects(ordered []*node, version uint64) []scheduledEffect {
	scheduled := make([]scheduledEffect, 0)
	for _, n := range ordered {
		if n == nil || n.kind != KindEffect || n.retired {
			continue
		}
		if _, err := failedDependency(n); err != nil {
			n.rt.emitEffectSkipped(n, err)
			continue
		}
		scheduled = append(scheduled, scheduledEffect{
			n:       n,
			version: version,
			values:  snapshotValues(n),
		})
	}
	return scheduled
}

func enqueueScheduledEffects(rt *Runtime, scheduled []scheduledEffect) {
	for _, item := range scheduled {
		rt.sched.enqueue(item.n, item.version, item.values)
	}
}

func collectAffected(changed []*node) map[*node]bool {
	_, affected := collectAffectedOrdered(changed)
	return affected
}

func collectAffectedOrdered(changed []*node) ([]*node, map[*node]bool) {
	affected := make(map[*node]bool)
	ordered := make([]*node, 0)
	queue := append([]*node(nil), changed...)
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		for _, dependent := range n.dependents {
			if !affected[dependent] {
				affected[dependent] = true
				ordered = append(ordered, dependent)
				queue = append(queue, dependent)
			}
		}
	}
	return ordered, affected
}

func reaches(from, target *node) bool {
	stack := []*node{from}
	seen := make(map[*node]bool)
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == target {
			return true
		}
		if seen[n] {
			continue
		}
		seen[n] = true
		stack = append(stack, n.dependents...)
	}
	return false
}
