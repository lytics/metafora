package metafora

import (
	"math"
	"math/rand"
	"time"
)

const (
	// Default threshold is 120% of cluster average
	defaultThreshold float32 = 1.2
)

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

// Provides information about the cluster to be used by FairBalancer
type ClusterState interface {
	// Provide the current number of jobs
	NodeTaskCount() (map[string]int, error)
}

func NewDefaultFairBalancer(nodeid string, cs ClusterState) Balancer {
	return NewDefaultFairBalancerWithThreshold(nodeid, cs, defaultThreshold)
}

func NewDefaultFairBalancerWithThreshold(nodeid string, cs ClusterState, threshold float32) Balancer {
	return &FairBalancer{
		nodeid:           nodeid,
		clusterstate:     cs,
		releaseThreshold: threshold,
		lastreleased:     map[string]bool{},
	}
}

// An implementation of `metafora.Balancer` which attempts to randomly release tasks in the case
// when the count of those currently running on this node is greater than some percentage of
// the cluster average (default 120%)
// This balancer will claim all tasks which were not released on the last call to Balance
type FairBalancer struct {
	nodeid string

	bc           BalancerContext
	clusterstate ClusterState

	releaseThreshold float32

	lastreleased map[string]bool
}

func (e *FairBalancer) Init(s BalancerContext) {
	e.bc = s
}

func (e *FairBalancer) CanClaim(taskid string) bool {
	// Skip those tasks which were last released
	return !e.lastreleased[taskid]
}

func (e *FairBalancer) Balance() []string {
	e.lastreleased = map[string]bool{}
	current, err := e.clusterstate.NodeTaskCount()
	if err != nil {
		e.bc.Log(LogLevelWarning, "Error retirieving cluster state: %v", err)
		return nil
	}

	releasetasks := []string{}
	shouldrelease := current[e.nodeid] - e.desiredCount(current)
	if shouldrelease < 1 {
		return nil
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	nodetasks := e.bc.Tasks()
	for len(releasetasks) <= shouldrelease {
		tid := nodetasks[random.Intn(len(nodetasks))]
		releasetasks = append(releasetasks, tid)
		e.lastreleased[tid] = true
	}

	return releasetasks
}

// Retrieve the desired maximum count, based on current cluster state
func (e *FairBalancer) desiredCount(current map[string]int) int {
	total := 0
	for _, c := range current {
		total += c
	}

	avg := total / len(current)

	return int(math.Ceil(float64(float32(avg) * e.releaseThreshold)))
}
