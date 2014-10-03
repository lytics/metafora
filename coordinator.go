package metafora

// CoordinatorContext is the context passed to coordinators by the core
// consumer.
type CoordinatorContext interface {
	Logger
}

type Coordinator interface {
	// Init is called once by the consumer to provide a Logger to Coordinator
	// implementations.
	Init(CoordinatorContext)

	// Watch should do a blocking watch on the broker and return a task ID that
	// can be claimed.
	Watch() (taskID string, err error)

	// Claim is called by the Consumer when a Balancer has determined that a task
	// ID can be claimed. Claim returns false if another consumer has already
	// claimed the ID.
	Claim(taskID string) bool

	// Command blocks until a command for this node is received from the broker
	// by the coordinator.
	Command() (cmd string, err error)

	Close() error
}
