package metafora

import (
	"math"
	"math/rand"
	"time"
)

const (
	// Default threshold is 120% of cluster average
	defaultThreshold float64 = 1.2
)

// BalancerContext is a limited interface exposed to Balancers from the
// Consumer for access to limited Consumer state.
type BalancerContext interface {
	// Tasks returns a sorted list of task IDs run by this Consumer. The Consumer
	// stops task manipulations during claiming and balancing, so the list will
	// be accurate unless a task naturally completes.
	Tasks() []Task
}

// Balancer is the core task balancing interface. Without a master Metafora
// clusters are cooperatively balanced -- meaning each node needs to know how
// to balance itself.
type Balancer interface {
	// Init is called once and only once before any other Balancer methods are
	// called. The context argument is meant to expose functionality that might
	// be useful for CanClaim and Balance implementations.
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

// NewDefaultFairBalancer creates a new FairBalancer but requires a
// ClusterState implementation to gain more information about the cluster than
// BalancerContext provides.
func NewDefaultFairBalancer(nodeid string, cs ClusterState) Balancer {
	return NewDefaultFairBalancerWithThreshold(nodeid, cs, defaultThreshold)
}

// NewDefaultFairBalancerWithThreshold allows callers to override
// FairBalancer's default 120% task load release threshold.
func NewDefaultFairBalancerWithThreshold(nodeid string, cs ClusterState, threshold float64) Balancer {
	return &FairBalancer{
		nodeid:           nodeid,
		clusterstate:     cs,
		releaseThreshold: threshold,
		lastreleased:     map[string]bool{},
	}
}

// An implementation of Balancer which attempts to randomly release tasks in
// the case when the count of those currently running on this node is greater
// than some percentage of the cluster average (default 120%).
//
// This balancer will claim all tasks which were not released on the last call
// to Balance.
type FairBalancer struct {
	nodeid string

	bc           BalancerContext
	clusterstate ClusterState

	releaseThreshold float64

	lastreleased map[string]bool
}

func (e *FairBalancer) Init(s BalancerContext) {
	e.bc = s
}

// CanClaim will claim all tasks, but will add a sleep to block claiming
// released tasks in order to give other nodes a chance to claim them first
func (e *FairBalancer) CanClaim(taskid string) bool {
	if e.lastreleased[taskid] {
		time.Sleep(500 * time.Millisecond)
	}
	return true
}

// Balance releases tasks if this node has 120% more tasks than the average
// node in the cluster.
func (e *FairBalancer) Balance() []string {
	e.lastreleased = map[string]bool{}
	current, err := e.clusterstate.NodeTaskCount()
	if err != nil {
		Warnf("Error retrieving cluster state: %v", err)
		return nil
	}

	releasetasks := []string{}
	shouldrelease := current[e.nodeid] - e.desiredCount(current)
	if shouldrelease < 1 {
		return nil
	}

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	nodetasks := e.bc.Tasks()
	for len(releasetasks) < shouldrelease {
		tid := nodetasks[random.Intn(len(nodetasks))].ID()
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

	avg := 0
	if len(current) > 0 {
		avg = total / len(current)
	}

	return int(math.Ceil(float64(avg) * e.releaseThreshold))
}
