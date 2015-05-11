package metafora

import (
	"encoding/json"
	"sync"
	"time"
)

type Task interface {
	ID() string
	Started() time.Time
	Stopped() time.Time
	Handler() Handler
}

// task is the per-task state Metafora tracks internally.
type task struct {
	// handler on which Run and Stop are called
	h Handler

	// id of task to satisfy Task interface
	id string

	// stopL serializes calls to task.h.Stop() to make handler implementations
	// easier/safer as well as guard stopped
	stopL sync.Mutex

	// when task was started and when Stop was first called
	started time.Time
	stopped time.Time
}

func newTask(id string, h Handler) *task {
	return &task{id: id, h: h, started: time.Now()}
}

func (t *task) stop() {
	t.stopL.Lock()
	defer t.stopL.Unlock()
	if t.stopped.IsZero() {
		t.stopped = time.Now()
	}
	t.h.Stop()
}

func (t *task) ID() string         { return t.id }
func (t *task) Handler() Handler   { return t.h }
func (t *task) Started() time.Time { return t.started }
func (t *task) Stopped() time.Time {
	t.stopL.Lock()
	defer t.stopL.Unlock()
	return t.stopped
}

func (t *task) MarshalJSON() ([]byte, error) {
	js := struct {
		ID      string     `json:"id"`
		Started time.Time  `json:"started"`
		Stopped *time.Time `json:"stopped,omitempty"`
	}{ID: t.id, Started: t.started}

	// Only set stopped if it's non-zero
	if s := t.Stopped(); !s.IsZero() {
		js.Stopped = &s
	}

	return json.Marshal(&js)
}
