package metafora

// Handler is the core task handling interface. The Consumer will create a new
// Handler for each claimed task, call Run once and only once, and call Stop
// when the task should persist its progress and exit.
type Handler interface {
	// Run handles a task and blocks until completion or Stop is called.
	//
	// If Run returns true, Metafora will mark the task as Done via the
	// Coordinator. The task will not be rescheduled.
	//
	// If Run returns false, Metafora will Release the task via the Coordinator.
	// The task will be scheduled to run again.
	//
	// Panics are treated the same as returning true.
	Run(taskID string) (done bool)

	// Stop signals to the handler to shutdown gracefully. Stop implementations
	// should not block until Run exits.
	//
	// Run probably wants to return false when stop is called, but this is left
	// up to the implementation as races between Run finishing and Stop being
	// called can happen.
	Stop()
}

// HandlerFunc is called by the Consumer to create a new Handler for each task.
type HandlerFunc func() Handler

// SimpleHander creates a HandlerFunc for a simple function that accepts a stop
// channel. The channel will be closed when Stop is called.
func SimpleHandler(f func(task string, stop <-chan bool) bool) HandlerFunc {
	return func() Handler {
		return &simpleHandler{
			stop: make(chan bool),
			f:    f,
		}
	}
}

type simpleHandler struct {
	stop chan bool
	f    func(string, <-chan bool) bool
}

func (h *simpleHandler) Run(task string) bool {
	return h.f(task, h.stop)
}

func (h *simpleHandler) Stop() {
	close(h.stop)
}
