package embedded

import (
	"sync"

	"github.com/lytics/metafora/statemachine"
)

// StateStore is an in-memory implementation of statemachine.StateStore
// intended for use in tests.
type StateStore struct {
	mu    *sync.RWMutex
	store map[string]*statemachine.State
}

func NewStateStore() statemachine.StateStore {
	return &StateStore{mu: &sync.RWMutex{}, store: map[string]*statemachine.State{}}
}

func (s *StateStore) Load(tid string) (*statemachine.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.store[tid]
	if !ok {
		return &statemachine.State{Code: statemachine.Runnable}, nil
	}
	return state, nil
}

func (s *StateStore) Store(tid string, state *statemachine.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[tid] = state
	return nil
}
