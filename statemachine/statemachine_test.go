package statemachine_test

import (
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/embedded"
	"github.com/lytics/metafora/statemachine"
)

func testhandler(tid string, cmds <-chan statemachine.Message) statemachine.Message {
	metafora.Debugf("Starting %s", tid)
	m := <-cmds
	metafora.Debugf("%s recvd %s", tid, m.Code)
	return m
}

type testStore struct {
	initial *statemachine.State
	out     chan<- *statemachine.State
}

func (s testStore) Load(taskID string) (*statemachine.State, error) {
	s.out <- s.initial
	return s.initial, nil
}
func (s testStore) Store(taskID string, newstate *statemachine.State) error {
	metafora.Debugf("%s storing %s", taskID, newstate.Code)
	s.out <- newstate
	return nil
}

//FIXME leaks goroutines
func TestRules(t *testing.T) {
	for i, trans := range statemachine.Rules {
		metafora.Debugf("Trying %s", trans)
		cmds := make(chan statemachine.Message)
		store := make(chan *statemachine.State)
		ts := testStore{initial: &statemachine.State{Code: trans.From}, out: store}

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

func TestCheckpointRelease(t *testing.T) {
	ss := embedded.NewStateStore()
	ss.Store("test1", &statemachine.State{Code: statemachine.Runnable})
	cmds := make(chan statemachine.Message)
	sm := statemachine.New(testhandler, ss, cmds, nil)
	done := make(chan bool)
	go func() { done <- sm.Run("test1") }()
	// Should just cause statemachine to loop
	cmds <- statemachine.Message{Code: statemachine.Checkpoint}
	select {
	case <-done:
		t.Fatalf("Checkpoint command should not have caused statemachine to exit.")
	case <-time.After(100 * time.Millisecond):
	}

	// Should cause the statemachine to exit
	cmds <- statemachine.Message{Code: statemachine.Release}
	select {
	case d := <-done:
		if d {
			t.Fatalf("Release command should not have caused the task to be marked as done.")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("Expected statemachine to exit but it did not.")
	}
	state, err := ss.Load("test1")
	if err != nil {
		t.Fatal(err)
	}
	if state.Code != statemachine.Runnable {
		t.Fatalf("Expected released task to be runnable but found state %q", state.Code)
	}
}
