package httputil

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

// Consumer contains just the Metafora methods exposed by the HTTP
// introspection endpoints.
type Consumer interface {
	Frozen() bool
	Tasks() []metafora.Task
	String() string
}

type stateMachine interface {
	State() (*statemachine.State, time.Time)
}

type Task struct {
	ID       string     `json:"id"`
	Started  time.Time  `json:"started"`
	Stopped  *time.Time `json:"stopped,omitempty"`
	State    string     `json:"state,omitempty"`
	Modified *time.Time `json:"modified,omitempty"`
}

// InfoResponse is the JSON response marshalled by the MakeInfoHandler.
type InfoResponse struct {
	Frozen  bool      `json:"frozen"`
	Name    string    `json:"name"`
	Started time.Time `json:"started"`
	Tasks   []Task    `json:"tasks"`
}

// MakeInfoHandler returns an HTTP handler which can be added to an exposed
// HTTP server mux by Metafora applications to provide operators with basic
// node introspection.
func MakeInfoHandler(c Consumer, started time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		tasks := c.Tasks()
		resp := InfoResponse{
			Frozen:  c.Frozen(),
			Name:    c.String(),
			Started: started,
			Tasks:   make([]Task, len(tasks)),
		}
		for i, task := range tasks {
			resp.Tasks[i] = Task{
				ID:      task.ID(),
				Started: task.Started(),
			}

			// Set stopped if it's non-zero
			stopped := task.Stopped()
			if !stopped.IsZero() {
				resp.Tasks[i].Stopped = &stopped
			}

			// Expose state if it exists
			if sh, ok := task.Handler().(stateMachine); ok {
				s, ts := sh.State()
				resp.Tasks[i].State = s.String()
				resp.Tasks[i].Modified = &ts
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&resp)
	}
}
