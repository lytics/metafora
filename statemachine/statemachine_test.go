package statemachine_test

import (
	"testing"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

type testHandler struct {
	done chan bool
}

func (h testHandler) Run(sm statemachine.StateMachine, taskID string) bool {
	<-h.done
	return true
}

func (h testHandler) Transition(t statemachine.Transition) error {
	close(h.done)
	return nil
}

type testStore struct {
	initial statemachine.State
}

func (s testStore) Load(taskID string) (statemachine.State, error) {
	return s.initial, nil
}
func (s testStore) Store(taskID string, _ statemachine.State) error {
	return nil
}

func TestRules(t *testing.T) {
	metafora.SetLogLevel(metafora.LogLevelDebug)
	for i, trans := range statemachine.Rules {
		// Create a new statemachine that starts from the From state
		sm := statemachine.New(testHandler{make(chan bool)}, testStore{statemachine.State{Code: trans.From}})
		go sm.Run("test")
		if sm.State().Code != trans.From {
			t.Fatalf("%d Initial state %q not set. Found: %q", i, trans.From, sm.State().Code)
		}

		// Apply the Event to transition to the To state
		state, err := sm.Apply(statemachine.Message{Code: trans.Event})
		if err != nil {
			t.Fatalf("%d %s -- Unexpected error: %v", i, trans.Event, err)
		}
		if state.Code != trans.To {
			t.Fatalf("%d Expected %q but found %q", i, trans.To, state.Code)
		}
	}
}
