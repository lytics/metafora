package m_etcd

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

// NewFairBalancer creates a new metafora.DefaultFairBalancer that uses etcd
// for counting tasks per node.
func NewFairBalancer(nodeid, namespace string, client *etcd.Client) metafora.Balancer {
	namespace = "/" + strings.Trim(namespace, "/ ")
	e := etcdClusterState{
		client:   client,
		taskPath: path.Join(namespace, "tasks"),
		nodePath: path.Join(namespace, "nodes"),
	}
	return metafora.NewDefaultFairBalancer(nodeid, &e)
}

// Checks the current state of an Etcd cluster
type etcdClusterState struct {
	client   *etcd.Client
	taskPath string
	nodePath string
}

func (e *etcdClusterState) NodeTaskCount() (map[string]int, error) {
	const sort = false
	const recursive = true
	state := map[string]int{}

	// First initialize state with nodes as keys
	resp, err := e.client.Get(e.nodePath, sort, recursive)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Node == nil {
		return state, nil
	}

	for _, node := range resp.Node.Nodes {
		state[path.Base(node.Key)] = 0
	}

	// Then count how many tasks each node has
	resp, err = e.client.Get(e.taskPath, sort, recursive)
	if err != nil {
		return nil, err
	}

	// No current tasks
	if resp == nil {
		return state, nil
	}

	// Get the list of all claimed work, create a map of the counts and
	// node values
	// We ignore tasks which have no claims
	for _, task := range resp.Node.Nodes {
		for _, claim := range task.Nodes {
			if path.Base(claim.Key) == OwnerMarker {
				val := ownerValue{}
				if err := json.Unmarshal([]byte(claim.Value), &val); err == nil {
					// We want to only include those nodes which were initially included,
					// as some nodes may be shutting down, etc, and should not be counted
					_, ok := state[val.Node]
					if ok {
						state[val.Node]++
					}
				}
			}
		}
	}

	return state, nil
}
