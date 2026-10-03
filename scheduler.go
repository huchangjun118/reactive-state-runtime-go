package reactive

import (
	"context"
	"time"
)

func newScheduler(rt *Runtime) *scheduler {
	s := &scheduler{
		rt:    rt,
		queue: make(chan task, rt.config.MaxEffectQueue),
		done:  make(chan struct{}),
	}
	for i := 0; i < rt.config.MaxConcurrentEffects; i++ {
		s.wg.Add(1)
		go s.worker()
	}
	return s
}

func (s *scheduler) worker() {
	defer s.wg.Done()
	for t := range s.queue {
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing {
			continue
		}
		s.run(t)
	}
}

func (s *scheduler) stop() {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.closing = true
	close(s.done)
	close(s.queue)
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *scheduler) enqueue(n *node, version uint64, values Values) {
	if n == nil || n.kind != KindEffect || n.retired {
		return
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return
	}
	es := n.sched
	if es == nil {
		return
	}
	values = append(Values(nil), values...)

	defer func() {
		// Closing the queue can race with a send; cancellation is the desired
		// outcome and must not escape as a panic.
		_ = recover()
	}()

	es.mu.Lock()
	if es.retired {
		es.mu.Unlock()
		return
	}
	es.latestVersion = version
	es.latestValues = values

	if es.running || es.queued {
		// A newer batch replaces the pending batch. The task already in the
		// queue or running is identified by an older run ID and discarded.
		es.runID++
		if es.running && es.cancel != nil {
			es.cancel()
		}
		es.mu.Unlock()
		return
	}

	es.queued = true
	es.runID++
	runID := es.runID
	es.mu.Unlock()

	t := task{effect: n, runID: runID, version: version, values: values}
	select {
	case s.queue <- t:
		return
	case <-s.done:
		if cleared, latestVersion, latestValues := s.clearQueued(n, runID); !cleared {
			s.enqueue(n, latestVersion, latestValues)
		}
		return
	default:
		if s.rt.config.QueuePolicy == QueuePolicyReject {
			if cleared, latestVersion, latestValues := s.clearQueued(n, runID); cleared {
				s.rt.metrics.EffectDrops.Add(1)
				if s.rt.observer.OnQueueDrop != nil {
					s.rt.observer.OnQueueDrop(n.id, version)
				}
			} else {
				s.enqueue(n, latestVersion, latestValues)
			}
			return
		}
	}

	select {
	case s.queue <- t:
	case <-s.done:
		if cleared, latestVersion, latestValues := s.clearQueued(n, runID); !cleared {
			s.enqueue(n, latestVersion, latestValues)
		}
	}
}

func (s *scheduler) clearQueued(n *node, runID uint64) (bool, uint64, Values) {
	es := n.sched
	if es == nil {
		return true, 0, nil
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	if es.runID == runID {
		es.queued = false
		es.dirty = false
		return true, es.latestVersion, es.latestValues
	}
	return false, es.latestVersion, es.latestValues
}

func (s *scheduler) run(t task) {
	es := t.effect.sched
	if es == nil {
		return
	}

	es.mu.Lock()
	if es.retired || t.runID != es.runID {
		latestVersion, latestValues, retired := es.latestVersion, es.latestValues, es.retired
		es.queued = false
		es.running = false
		es.dirty = false
		es.mu.Unlock()
		if !retired {
			s.enqueue(t.effect, latestVersion, latestValues)
		}
		return
	}
	es.running = true
	es.queued = false
	es.dirty = false
	es.mu.Unlock()

	s.rt.metrics.EffectRuns.Add(1)
	ctx, cancel := effectContext(context.Background(), s.rt.config.EffectTimeout)
	es.mu.Lock()
	es.cancel = cancel
	es.mu.Unlock()

	err := safeEffect(t.effect.effectFn, ctx, t.values)

	es.mu.Lock()
	es.cancel = nil
	cancel()
	retired := es.retired
	stale := t.runID != es.runID
	es.running = false
	es.queued = false
	dirty := es.dirty
	latestVersion, latestValues := es.latestVersion, es.latestValues
	if stale {
		es.dirty = false
	}
	es.mu.Unlock()

	if retired {
		return
	}
	if err != nil && !stale {
		s.rt.metrics.EffectErrors.Add(1)
		if s.rt.observer.OnEffectError != nil {
			s.rt.observer.OnEffectError(t.effect.id, t.version, err)
		}
	}
	if stale || dirty {
		s.enqueue(t.effect, latestVersion, latestValues)
	}
}

func safeEffect(fn func(context.Context, Values) error, ctx context.Context, values Values) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError{value: r}
		}
	}()
	return fn(ctx, values)
}

func effectContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

type panicError struct{ value any }

func (e panicError) Error() string {
	return "effect panicked"
}
