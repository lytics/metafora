package metcdv3

import (
	"context"
	"encoding/json"
	"path"

	"github.com/lytics/metafora"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

// NewFairBalancer creates a new metafora.DefaultFairBalancer that uses etcd
// for counting tasks per node.
func NewFairBalancer(conf *Config, etcdv3c *etcdv3.Client, filter func(*FilterableValue) bool) metafora.Balancer {
	e := etcdClusterState{
		etcdv3c:  etcdv3c,
		kvc:      etcdv3.NewKV(etcdv3c),
		taskPath: path.Join(conf.Namespace, TasksPath),
		nodePath: path.Join(conf.Namespace, NodesPath),
		filter:   filter,
	}
	return metafora.NewDefaultFairBalancer(conf.Name, &e)
}

// Checks the current state of an Etcd cluster
type etcdClusterState struct {
	etcdv3c  *etcdv3.Client
	kvc      etcdv3.KV
	taskPath string
	nodePath string
	filter   func(*FilterableValue) bool
}

type FilterableValue struct {
	Name string
}

func (e *etcdClusterState) NodeTaskCount() (map[string]int, error) {
	state := map[string]int{}

	// First initialize state with nodes as keys
	resp, err := e.kvc.Get(context.Background(), e.nodePath, etcdv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if resp == nil || len(resp.Kvs) == 0 {
		metafora.Warnf("balancer received empty response from GET %s", e.nodePath)
		return state, nil
	}

	for _, kv := range resp.Kvs {
		// We're guarunteed to find nodes under the _metadata path (created on Coordinator startup)
		dir, _ := path.Split(string(kv.Key))
		dir, node := path.Split(path.Clean(dir))
		if path.Clean(dir) == e.nodePath && e.filter(&FilterableValue{Name: node}) {
			state[node] = 0
		}
	}

	resp, err = e.kvc.Get(context.Background(), e.taskPath, etcdv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	// No current tasks
	if resp == nil || len(resp.Kvs) == 0 {
		return state, nil
	}

	// Get the list of all claimed work, create a map of the counts and
	// node values
	// We ignore tasks which have no claims
	for _, kv := range resp.Kvs {
		ownerPath := path.Base(string(kv.Key))
		if ownerPath == OwnerPath {
			ov := &ownerValue{}
			err := json.Unmarshal(kv.Value, ov)
			if err != nil {
				return nil, err
			}
			// We want to only include those nodes which were initially included,
			// as some nodes may be shutting down, etc, and should not be counted
			if _, ok := state[ov.Node]; ok {
				state[ov.Node]++
			}
		}
	}
	return state, nil
}
