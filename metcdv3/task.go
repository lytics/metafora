package metcdv3

import "github.com/lytics/metafora"

type task struct {
	id string
}

func (t *task) ID() string { return t.id }

// TaskFunc creates a Task interface from a task ID and etcd Node. The Node
// corresponds to the task directory.
//
// Implementations must support value being an empty string.
//
// If nil is returned the task is ignored.
type TaskFunc func(id, value string) metafora.Task

// DefaultTaskFunc is the default new task function used by the EtcdCoordinator
// and does not attempt to process the properties value.
func DefaultTaskFunc(id, _ string) metafora.Task { return &task{id: id} }
