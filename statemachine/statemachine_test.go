package statemachine_test

import (
	"testing"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

func testhandler(tid string, cmds <-chan statemachine.Message) statemachine.Message {
	metafora.Debugf("Starting %s", tid)
	m := <-cmds
	metafora.Debugf("%s recvd %s", tid, m.Code)
	return m
}

type testStore struct {
	initial statemachine.State
	out     chan<- statemachine.State
}

func (s testStore) Load(taskID string) (statemachine.State, error) {
	s.out <- s.initial
	return s.initial, nil
}
func (s testStore) Store(taskID string, newstate statemachine.State) error {
	metafora.Debugf("%s storing %s", taskID, newstate.Code)
	s.out <- newstate
	return nil
}

//FIXME leaks goroutines
func TestRules(t *testing.T) {
	metafora.SetLogLevel(metafora.LogLevelDebug)

	for i, trans := range statemachine.Rules {
		metafora.Debugf("Trying %s", trans)
		cmds := make(chan statemachine.Message)
		store := make(chan statemachine.State)
		ts := testStore{initial: statemachine.State{Code: trans.From}, out: store}

		// Create a new statemachine that starts from the From state
		sm := statemachine.New(testhandler, ts, cmds, nil)
		go sm.Run("test")
		initial := <-store
		if initial.Code != trans.From {
			t.Fatalf("%d Initial state %q not set. Found: %q", i, trans.From, initial.Code)
		}

		// The Fault state transitions itself to either sleeping or failed
		if trans.From != statemachine.Fault {
			// Apply the Event to transition to the To state
			cmds <- statemachine.Message{Code: trans.Event}
		}
		newstate := <-store
		if trans.From == statemachine.Fault && trans.To == statemachine.Failed {
			//FIXME continue on as this transition relies on state this test doesn't exercise
			continue
		}
		if newstate.Code != trans.To {
			t.Fatalf("%d Expected %q but found %q", i, trans.To, newstate.Code)
		}
	}
}
