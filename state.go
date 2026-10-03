package reactive

// State is a mutable root value in the dependency graph.
type State[T any] struct {
	n *node
}

// NewState registers a root state with an initial committed value.
func NewState[T any](rt *Runtime, initial T) *State[T] {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return nil
	}
	n := rt.newNode(KindState)
	n.value = initial
	return &State[T]{n: n}
}

// Set stages a state value in a transaction.
func (s *State[T]) Set(tx *Tx, value T) {
	tx.set(s.n, value)
}

// Get returns the latest committed value and version.
func (s *State[T]) Get() (T, uint64) {
	s.n.rt.mu.Lock()
	defer s.n.rt.mu.Unlock()
	value, _ := s.n.value.(T)
	return value, s.n.rt.version
}

// ID returns the stable node identifier.
func (s *State[T]) ID() ID { return s.n.id }

func (s *State[T]) node() *node { return s.n }
