package httputil

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/lytics/metafora"
)

// Consumer contains just the Metafora methods exposed by the HTTP
// introspection endpoints.
type Consumer interface {
	Frozen() bool
	Tasks() []metafora.Task
	String() string
}

// InfoResponse is the JSON response marshalled by the MakeInfoHandler.
type InfoResponse struct {
	Frozen  bool            `json:"frozen"`
	Name    string          `json:"name"`
	Started time.Time       `json:"started"`
	Tasks   []metafora.Task `json:"tasks"`
}

// MakeInfoHandler returns an HTTP handler which can be added to an exposed
// HTTP server mux by Metafora applications to provide operators with basic
// node introspection.
func MakeInfoHandler(c Consumer, started time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&InfoResponse{
			Frozen:  c.Frozen(),
			Name:    c.String(),
			Started: started,
			Tasks:   c.Tasks(),
		})
	}
}
