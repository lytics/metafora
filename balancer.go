package metafora

type Balancer interface {
	// Init is called once and only once with an interface for use by CanClaim
	// and Balance to use the state of the Consumer.
	Init(ConsumerState)

	// CanClaim should return true if the consumer should accept a task. No new
	// tasks will be claimed while CanClaim is called.
	CanClaim(taskID string) bool

	// Balance should return the list of Task IDs that should be released. No new
	// tasks will be claimed during balancing.
	Balance() (release []string)
}

type DumbBalancer struct{}

func (*DumbBalancer) Init(ConsumerState)   {}
func (*DumbBalancer) CanClaim(string) bool { return true }
func (*DumbBalancer) Balance() []string    { return nil }
