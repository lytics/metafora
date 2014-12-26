package m_etcd

import (
	"encoding/json"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

// NewFairBalancer creates a new metafora.DefaultFairBalancer that uses etcd
// for counting tasks per node.
func NewFairBalancer(nodeid, namespace string, client *etcd.Client) metafora.Balancer {
	return metafora.NewDefaultFairBalancer(nodeid, &etcdClusterState{client, namespace})
}

// Checks the current state of an Etcd cluster
type etcdClusterState struct {
	client    *etcd.Client
	namespace string
}

func (e *etcdClusterState) NodeTaskCount() (map[string]int, error) {
	const sort = false
	const recursive = true
	resp, err := e.client.Get(e.namespace+"/tasks/", sort, recursive)
	if err != nil {
		return nil, err
	}

	state := map[string]int{}

	// No current tasks
	if resp == nil {
		return state, nil
	}

	// Get the list of all claimed work, create a map of the counts and
	// node values
	// We ignore tasks which have no claims
	for _, task := range resp.Node.Nodes {
		for _, claim := range task.Nodes {
			if claim.Key == OwnerMarker {
				val := ownerValue{}
				if err := json.Unmarshal([]byte(claim.Value), &val); err != nil {
					state[val.Node]++
				}
			}
		}
	}

	return state, nil
}
