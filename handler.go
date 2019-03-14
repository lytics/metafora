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
	Run() (done bool)

	// Stop signals to the handler to shutdown gracefully. Stop implementations
	// should not block until Run exits.
	//
	// Stop may be called more than once but calls are serialized. Implmentations
	// may perform different operations on subsequent calls to Stop to implement
	// graceful vs. forced shutdown conditions.
	//
	// Run probably wants to return false when stop is called, but this is left
	// up to the implementation as races between Run finishing and Stop being
	// called can happen.
	Stop()
}

// HandlerFunc is called by the Consumer to create a new Handler for each task.
//
// HandlerFunc is meant to be the New function for handlers. Since Run and Stop
// are called concurrently, any state used by both should be initialized in the
// HandlerFunc. Since Handlerfunc is uninterruptable, only the minimum amount
// of work necessary to initialize a handler should be done.
type HandlerFunc func(Task) Handler

// SimpleHandler creates a HandlerFunc for a simple function that accepts a stop
// channel. The channel will be closed when Stop is called.
func SimpleHandler(f func(t Task, stop <-chan bool) bool) HandlerFunc {
	return func(t Task) Handler {
		return &simpleHandler{
			task: t,
			stop: make(chan bool),
			f:    f,
		}
	}
}

type simpleHandler struct {
	task Task
	stop chan bool
	f    func(Task, <-chan bool) bool
}

func (h *simpleHandler) Run() bool {
	return h.f(h.task, h.stop)
}

func (h *simpleHandler) Stop() {
	select {
	case <-h.stop:
	default:
		close(h.stop)
	}
}
