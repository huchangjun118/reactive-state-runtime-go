package reactive

import "sync"

// task 是一次副作用执行请求。
type task struct {
	run    func() // 实际执行体
	onDrop func() // 被丢弃（队列满 DropOldest）时回调，用于清理状态
}

// scheduler 是有界并发、有界队列的执行器。
// 调度顺序严格为 FIFO，不依赖 map 遍历、时间或随机量。
type scheduler struct {
	maxConc int
	maxQ    int
	policy  OverflowPolicy

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []*task
	running int
	closed  bool
	wg      sync.WaitGroup
}

func newScheduler(maxConc, maxQ int, policy OverflowPolicy) *scheduler {
	s := &scheduler{maxConc: maxConc, maxQ: maxQ, policy: policy}
	s.cond = sync.NewCond(&s.mu)
	s.wg.Add(1)
	go s.dispatch()
	return s
}

// submit 提交任务；队列满时按策略拒绝（ErrQueueFull）或丢弃最旧任务。
// 被丢弃的任务通过返回值交还调用方处理（避免在调度器锁内执行回调）。
func (s *scheduler) submit(t *task) (dropped *task, err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if len(s.queue) >= s.maxQ {
		if s.policy == Reject {
			s.mu.Unlock()
			return nil, ErrQueueFull
		}
		// DropOldest：丢弃队首任务。
		dropped = s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
	}
	s.queue = append(s.queue, t)
	s.cond.Signal()
	s.mu.Unlock()
	return dropped, nil
}

// dispatch 按 FIFO 顺序派发任务，并发度不超过 maxConc。
func (s *scheduler) dispatch() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for !s.closed && (len(s.queue) == 0 || s.running >= s.maxConc) {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		t := s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
		s.running++
		s.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				s.running--
				s.cond.Broadcast()
				s.mu.Unlock()
			}()
			t.run()
		}()
	}
}

// waitIdle 等待队列清空且无在途任务（不阻止新任务提交）。
func (s *scheduler) waitIdle() {
	s.mu.Lock()
	for len(s.queue) > 0 || s.running > 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
}

// close 停止派发并等待在途任务结束；未执行的排队任务被丢弃并返回给调用方。
func (s *scheduler) close() (dropped []*task) {
	s.mu.Lock()
	s.closed = true
	dropped = s.queue
	s.queue = nil
	s.cond.Broadcast()
	s.mu.Unlock()
	s.wg.Wait()
	return dropped
}
