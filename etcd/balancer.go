package etcd

import (
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"math"
	"math/rand"
)

const (
	defaultThreshold float32 = 1.2
)

var (
	_ metafora.Balancer = (*FairBalancer)(nil)
)

// Provides information about the cluster to be used by FairBalancer
type ClusterState interface {
	// Provide the current number of jobs
	NodeTaskCount() (map[string]int, error)
}

func NewEtcdSharedBalancer(nodeid, namespace string, client *etcd.Client) *FairBalancer {
	return NewDefaultFairBalancer(nodeid, &EtcdClusterState{client, namespace})
}

// Checks the current state of an Etcd cluster
type EtcdClusterState struct {
	client    *etcd.Client
	namespace string
}

func (e *EtcdClusterState) NodeTaskCount() (map[string]int, error) {
	resp, err := e.client.Get(fmt.Sprintf("%s/task/", e.namespace), false, true)
	if err != nil {
		return nil, err
	}

	// No current tasks
	if resp == nil {
		return map[string]int{}, nil
	}

	newstate := map[string]int{}
	// Get the list of all claimed work, create a map of the counts and
	// node values
	// We ignore tasks which have no claims TODO?
	for _, task := range resp.Node.Nodes {
		for _, claim := range task.Nodes {
			newstate[claim.Value]++
		}
	}

	return newstate, nil
}

func NewDefaultFairBalancer(nodeid string, cs ClusterState) *FairBalancer {
	return &FairBalancer{
		nodeid:           nodeid,
		clusterstate:     cs,
		releaseThreshold: defaultThreshold,
		lastreleased:     map[string]bool{},
	}
}

// An implementation of `metafora.Balancer` which attempts to release tasks in the case
// when the count of those currently running is greater than the average (multiplied by
// the current threshold)
// This balancer will claim all tasks which were not released on the last call to Balance
type FairBalancer struct {
	nodeid string

	bc           metafora.BalancerContext
	clusterstate ClusterState

	releaseThreshold float32

	lastreleased map[string]bool
}

func (e *FairBalancer) Init(s metafora.BalancerContext) {
	e.bc = s
}

func (e *FairBalancer) CanClaim(taskid string) bool {
	// Don't claim jobs unless .Init() has been called
	if e.bc == nil {
		return false
	}

	// Skip those tasks which were last released
	return !e.lastreleased[taskid]
}

func (e *FairBalancer) Balance() []string {
	e.lastreleased = map[string]bool{}
	current, err := e.clusterstate.NodeTaskCount()
	if err != nil {
		return nil
	}

	releasetasks := []string{}
	shouldrelease := current[e.nodeid] - e.desiredCount(current)
	if shouldrelease < 1 {
		return nil
	}

	nodetasks := e.bc.Tasks()
	for len(releasetasks) <= shouldrelease {
		tid := nodetasks[rand.Intn(len(nodetasks))]
		releasetasks = append(releasetasks, tid)
		e.lastreleased[tid] = true
	}

	return releasetasks
}

// Retrieve the `desired` maximum count, based on current cluster state
func (e *FairBalancer) desiredCount(current map[string]int) int {
	total := 0
	for _, c := range current {
		total += c
	}

	avg := total / len(current)

	return int(math.Ceil(float64(float32(avg) * e.releaseThreshold)))
}
