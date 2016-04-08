package m_etcd

import (
	"encoding/json"
	"path"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

const statePath = "state"

// stateStore is an etcd implementation of statemachine.StateStore.
type stateStore struct {
	c    client.KeysAPI
	path string
}

// NewStateStore returns a StateStore implementation that persists task states
// in etcd.
func NewStateStore(conf *Config) statemachine.StateStore {
	c, _ := newEtcdClient(conf)
	return &stateStore{
		c:    c,
		path: path.Join(conf.Namespace, statePath),
	}
}

// Load retrieves the given task's state from etcd or stores and returns
// Runnable if no state exists.
func (s *stateStore) Load(task metafora.Task) (*statemachine.State, error) {
	const notrecursive = false
	const nosort = false
	resp, err := s.c.Get(context.TODO(), path.Join(s.path, task.ID()), getNoSortNoRecur)
	if err != nil {
		if client.IsKeyNotFound(err) {
			metafora.Infof("task=%q has no existing state, default to Runnable", task.ID())
			state := &statemachine.State{Code: statemachine.Runnable}
			if err := s.Store(task, state); err != nil {
				return nil, err
			}
			return state, nil
		}

		// Non-404 error, fail
		return nil, err
	}

	// Unmarshal state from key
	state := statemachine.State{}
	if err := json.Unmarshal([]byte(resp.Node.Value), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// Store taskID's state in etcd overwriting any prior state.
func (s *stateStore) Store(task metafora.Task, state *statemachine.State) error {
	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}

	_, err = s.c.Set(context.TODO(), path.Join(s.path, task.ID()), string(buf), setDefault)
	return err
}
