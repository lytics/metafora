package metafora

// CoordinatorContext is the context passed to coordinators by the core
// consumer.
type CoordinatorContext interface {
	// Lost is called by the Coordinator when a claimed task is lost to another
	// node. The Consumer will stop the task locally.
	//
	// Since this implies there is a window of time where the task is executing
	// more than once, this is a sign of an unhealthy cluster.
	Lost(Task)
}

// Coordinator is the core interface Metafora uses to discover, claim, and
// tasks as well as receive commands.
type Coordinator interface {
	// Init is called once by the consumer to provide a Logger to Coordinator
	// implementations. NewConsumer will return Init's return value.
	Init(CoordinatorContext) error

	// Watch the broker for claimable tasks. Watch blocks until Close is called
	// or it encounters an error. Tasks are sent to consumer via the tasks chan.
	Watch(tasks chan<- Task) (err error)

	// Claim is called by the Consumer when a Balancer has determined that a task
	// ID can be claimed. Claim returns false if another consumer has already
	// claimed the ID.
	Claim(Task) bool

	// Release a task for other consumers to claim. May be called after Close.
	Release(Task)

	// Done is called by Metafora when a task has been completed and should never
	// be scheduled to run again (in other words: deleted from the broker).
	//
	// May be called after Close.
	Done(Task)

	// Command blocks until a command for this node is received from the broker
	// by the coordinator. Command must return (nil, nil) when Close is called.
	Command() (Command, error)

	// Close the coordinator. Stop waiting for tasks and commands. Remove node from broker.
	//
	// Do not release tasks. The consumer will handle task releasing.
	Close()

	// Name of the coordinator for use in logs and other tooling.
	Name() string
}

type coordinatorContext struct {
	*Consumer
}

// Lost is a light wrapper around Coordinator.stopTask to make it suitable for
// calling by Coordinator implementations via the CoordinatorContext interface.
func (ctx *coordinatorContext) Lost(t Task) {
	tid := t.ID()
	Errorf("Lost task %s", tid)
	ctx.stopTask(tid)
}
