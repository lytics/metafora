package metafora

// BalancerContext is a limited interface exposed to Balancers from the
// Consumer for access to logging and limited Consumer state.
type BalancerContext interface {
	// Tasks returns a sorted list of task IDs run by this Consumer. The Consumer
	// stops task manipulations during claiming and balancing, so the list will
	// be accurate unless a task naturally completes.
	Tasks() []string

	Logger
}

type Balancer interface {
	// Init is called once and only once with an interface for use by CanClaim
	// and Balance to use the state of the Consumer.
	Init(BalancerContext)

	// CanClaim should return true if the consumer should accept a task. No new
	// tasks will be claimed while CanClaim is called.
	CanClaim(taskID string) bool

	// Balance should return the list of Task IDs that should be released. No new
	// tasks will be claimed during balancing. The criteria used to determine
	// which tasks should be released is left up to the implementation.
	Balance() (release []string)
}

// DumbBalancer is the simplest possible balancer implementation which simply
// accepts all tasks.
type DumbBalancer struct{}

// Init does nothing.
func (*DumbBalancer) Init(BalancerContext) {}

// CanClaim always returns true.
func (*DumbBalancer) CanClaim(string) bool { return true }

// Balance never returns any tasks to balance.
func (*DumbBalancer) Balance() []string { return nil }
