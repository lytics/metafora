package statemachine

import "github.com/lytics/metafora"

// StateStore is an interface implementations must provide for persisting task
// state. Since the task ID is provided on each method call a single global
// StateStore can be used and implementations should be safe for concurrent
// access.
type StateStore interface {
	// Load the persisted or initial state for a task. Errors will cause tasks to
	// be marked as done.
	//
	// The one exception is the special error StateNotFound which will cause the
	// state machine to start from the initial (Runnable) state.
	Load(metafora.Task) (*State, error)

	// Store the current task state. Errors will prevent current state from being
	// persisted and prevent state transitions.
	Store(metafora.Task, *State) error
}
