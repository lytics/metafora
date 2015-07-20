package metafora

import (
	"encoding/json"
	"sync"
	"time"
)

// Task is the minimum interface for Tasks to implement.
type Task interface {
	// ID is the immutable globally unique ID for this task.
	ID() string
}

// RunningTask represents tasks running within a consumer.
type RunningTask interface {
	Task() Task

	// Started is the time the task was started by this consumer.
	Started() time.Time

	// Stopped is the first time Stop() was called on this task or zero is it has
	// yet to be called. Tasks may take an indeterminate amount of time to
	// shutdown after Stop() is called.
	Stopped() time.Time

	// Handler implementation called for this task.
	Handler() Handler
}

// runtask is the per-task state Metafora tracks internally.
type runtask struct {
	// task is the original Task from the coordinator
	task Task

	// handler on which Run and Stop are called
	h Handler

	// stopL serializes calls to task.h.Stop() to make handler implementations
	// easier/safer as well as guard stopped
	stopL sync.Mutex

	// when task was started and when Stop was first called
	started time.Time
	stopped time.Time
}

func newTask(task Task, h Handler) *runtask {
	return &runtask{task: task, h: h, started: time.Now()}
}

func (t *runtask) stop() {
	t.stopL.Lock()
	defer t.stopL.Unlock()
	if t.stopped.IsZero() {
		t.stopped = time.Now()
	}
	t.h.Stop()
}

func (t *runtask) Task() Task         { return t.task }
func (t *runtask) Handler() Handler   { return t.h }
func (t *runtask) Started() time.Time { return t.started }
func (t *runtask) Stopped() time.Time {
	t.stopL.Lock()
	defer t.stopL.Unlock()
	return t.stopped
}

func (t *runtask) MarshalJSON() ([]byte, error) {
	js := struct {
		ID      string     `json:"id"`
		Started time.Time  `json:"started"`
		Stopped *time.Time `json:"stopped,omitempty"`
	}{ID: t.task.ID(), Started: t.started}

	// Only set stopped if it's non-zero
	if s := t.Stopped(); !s.IsZero() {
		js.Stopped = &s
	}

	return json.Marshal(&js)
}
