package metcdv3

import (
	"context"
	"encoding/json"
	"path"

	etcdv3 "go.etcd.io/etcd/clientv3"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

const statePath = "state"

// stateStore is an etcd implementation of statemachine.StateStore.
type stateStore struct {
	etcdv3c *etcdv3.Client
	kvc     etcdv3.KV
	path    string
}

// NewStateStore returns a StateStore implementation that persists task states
// in etcd.
func NewStateStore(namespace string, etcdv3c *etcdv3.Client) statemachine.StateStore {
	return &stateStore{
		etcdv3c: etcdv3c,
		kvc:     etcdv3.NewKV(etcdv3c),
		path:    path.Join("/", namespace, statePath),
	}
}

// Load retrieves the given task's state from etcd or stores and returns
// Runnable if no state exists.
func (s *stateStore) Load(task metafora.Task) (*statemachine.State, error) {
	resp, err := s.kvc.Get(context.Background(), path.Join(s.path, task.ID()), etcdv3.WithLimit(1))
	if err != nil {
		return nil, err

	}

	if resp.Count == 0 {
		metafora.Infof("task=%q has no existing state, default to Runnable", task.ID())
		state := &statemachine.State{Code: statemachine.Runnable}
		if err := s.Store(task, state); err != nil {
			return nil, err
		}
		return state, nil
	}

	// Unmarshal state from key
	state := &statemachine.State{}
	if err := json.Unmarshal([]byte(resp.Kvs[0].Value), state); err != nil {
		return nil, err
	}
	return state, nil
}

// Store taskID's state in etcd overwriting any prior state.
func (s *stateStore) Store(task metafora.Task, state *statemachine.State) error {
	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}

	_, err = s.kvc.Put(context.Background(), path.Join(s.path, task.ID()), string(buf))
	return err
}
