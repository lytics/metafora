package metafora

// CoordinatorContext is the context passed to coordinators by the core
// consumer.
type CoordinatorContext interface {
	// Lost is called by the Coordinator when a claimed task is lost to another
	// node. The Consumer will stop the task locally.
	//
	// Since this implies there is a window of time where the task is executing
	// more than once, this is a sign of an unhealthy cluster.
	Lost(taskID string)
	Logger
}

// Coordinator is the core interface Metafora uses to discover, claim, and
// tasks as well as receive commands.
type Coordinator interface {
	// Init is called once by the consumer to provide a Logger to Coordinator
	// implementations. NewConsumer will return Init's return value.
	Init(CoordinatorContext) error

	// Watch the broker for claimable tasks. Returned tasks are passed to the
	// Balancer to check if a claim should be attempted.
	//
	// Watch returns ("", nil) when the Coordinator has been Closed.
	Watch() (taskID string, err error)

	// Claim is called by the Consumer when a Balancer has determined that a task
	// ID can be claimed. Claim returns false if another consumer has already
	// claimed the ID.
	Claim(taskID string) bool

	// Release a task for other consumers to claim.
	Release(taskID string)

	// Done is called by Metafora when a task has been completed and should never
	// be scheduled to run again (in other words: deleted from the broker).
	Done(taskID string)

	// Command blocks until a command for this node is received from the broker
	// by the coordinator. Command must return (nil, nil) when Close is called.
	Command() (Command, error)

	// Close indicates the Coordinator should stop watching and receiving
	// commands. It is called during Consumer.Shutdown().
	Close()
}

type coordinatorContext struct {
	*Consumer
	Logger
}

// Lost is a light wrapper around Coordinator.stopTask to make it suitable for
// calling by Coordinator implementations via the CoordinatorContext interface.
func (ctx *coordinatorContext) Lost(taskID string) {
	ctx.Log(LogLevelError, "Lost task %s", taskID)
	if !ctx.stopTask(taskID) {
		ctx.Log(LogLevelWarn, "Lost task %s wasn't running.", taskID)
	}
}
