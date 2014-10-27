package metafora

// Handler is the core task handling interface. The Consumer will create a new
// Handler for each claimed task, call Run once and only once, and call Stop
// when the task should persist its progress and exit.
type Handler interface {
	// Run should block until a task is complete. If it returns nil, the task is
	// considered complete. If error is non-nil, ...well... log it? FIXME
	Run(taskID string) error

	// Stop should signal to the handler to shutdown gracefully. Stop
	// implementations should not block until Run exits.
	Stop()
}

// HandlerFunc is called by the Consumer to create a new Handler for each task.
type HandlerFunc func() Handler

// FatalError is a custom error interface Handlers may choose to return from
// their Run methods in order to indicate to Metafora that the task has failed
// and should not be rescheduled.
//
// If an error is returned by Run that does not implement this interface, or
// Fatal() returns false, the task will be rescheduled.
type FatalError interface {
	error

	// Fatal returns true when an error is unrecoverable and should not be
	// rescheduled.
	Fatal() bool
}
