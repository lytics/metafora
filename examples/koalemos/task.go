package koalemos

import "github.com/lytics/metafora"

// Not necessary; just here to illustrate the point of this struct
var _ metafora.Task = (*Task)(nil)

type Task struct {
	id   string
	Args []string
}

func (t *Task) ID() string { return t.id }

// NewTask creates a new task given an ID.
func NewTask(id string) *Task {
	return &Task{id: id}
}
