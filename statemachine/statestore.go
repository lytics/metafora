package statemachine

import "github.com/lytics/metafora"

// StateStore is an interface implementations must provide for persisting task
// state. Since the task ID is provided on each method call a single global
// StateStore can be used and implementations should be safe for concurrent
// access.
type StateStore interface {
	// Load the persisted or initial state for a task. Returning ReleasableError
	// itself releases the task for retry; any other error marks it as done.
	Load(metafora.Task) (*State, error)

	// Store the current task state. Errors will prevent current state from being
	// persisted and prevent state transitions. An error wrapping ReleasableError
	// releases the task for retry; any other error marks it as done.
	Store(metafora.Task, *State) error
}
