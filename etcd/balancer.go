package etcd

import (
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"math"
	"math/rand"
)

func NewEtcdFairBalancer(nodeid, namespace string, client *etcd.Client) *FairBalancer {
	return NewDefaultFairBalancer(nodeid, &EtcdClusterState{client, namespace})
}

// Checks the current state of an Etcd cluster
type etcdClusterState struct {
	client    *etcd.Client
	namespace string
}

func (e *etcdClusterState) NodeTaskCount() (map[string]int, error) {
	const sort = false
	const recursive = true
	resp, err := e.client.Get(fmt.Sprintf("%s/task/", e.namespace), sort, recursive)
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
	// We ignore tasks which have no claims
	for _, task := range resp.Node.Nodes {
		for _, claim := range task.Nodes {
			newstate[claim.Value]++
		}
	}

	return newstate, nil
}
