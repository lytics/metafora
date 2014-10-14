package metafora

// CoordinatorContext is the context passed to coordinators by the core
// consumer.
type CoordinatorContext interface {
	Logger
}

// Coordinator is the core interface Metafora uses to discover, claim, and
// tasks as well as receive commands.
type Coordinator interface {
	// Init is called once by the consumer to provide a Logger to Coordinator
	// implementations.
	Init(CoordinatorContext)

	// Watch should do a blocking watch on the broker and return a task ID that
	// can be claimed. Watch must return ("", nil) when Close or Freeze are
	// called.
	Watch() (taskID string, err error)

	// Claim is called by the Consumer when a Balancer has determined that a task
	// ID can be claimed. Claim returns false if another consumer has already
	// claimed the ID.
	Claim(taskID string) bool

	// Release a task for other consumers to claim.
	Release(taskID string)

	// Command blocks until a command for this node is received from the broker
	// by the coordinator. Command must return (nil, nil) when Close is called.
	Command() (Command, error)

	// Close indicates the Coordinator should stop watching and receiving
	// commands. It is called during Consumer.Shutdown().
	Close()
}
