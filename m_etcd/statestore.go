package m_etcd

import (
	"encoding/json"
	"path"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

const statePath = "state"

// stateStore is an etcd implementation of statemachine.StateStore.
type stateStore struct {
	c    *etcd.Client
	path string
}

// NewStateStore returns a StateStore implementation that persists task states
// in etcd.
func NewStateStore(namespace string, etcdc *etcd.Client) statemachine.StateStore {
	if namespace[0] != '/' {
		namespace = "/" + namespace
	}
	return &stateStore{
		c:    etcdc,
		path: path.Join(namespace, statePath),
	}
}

// Load retrieves the given task's state from etcd or stores and returns
// Runnable if no state exists.
func (s *stateStore) Load(task metafora.Task) (*statemachine.State, error) {
	const notrecursive = false
	const nosort = false
	resp, err := s.c.Get(path.Join(s.path, task.ID()), notrecursive, nosort)
	if err != nil {
		if ee, ok := err.(*etcd.EtcdError); ok && ee.ErrorCode == EcodeKeyNotFound {
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

	_, err = s.c.Set(path.Join(s.path, task.ID()), string(buf), ForeverTTL)
	return err
}
