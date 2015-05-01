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
	t.Parallel()
	for i, trans := range statemachine.Rules {
		metafora.Debugf("Trying %s", trans)
		cmdr := embedded.NewCommander()
		cmdlistener := cmdr.NewListener("test")
		store := make(chan *statemachine.State)

		state := &statemachine.State{Code: trans.From}

		// Sleeping state needs extra Until state
		if trans.From == statemachine.Sleeping {
			until := time.Now().Add(100 * time.Millisecond)
			state.Until = &until
		}

		ts := testStore{initial: state, out: store}

		// Create a new statemachine that starts from the From state
		sm := statemachine.New("test", testhandler, ts, cmdlistener, nil)
		go sm.Run()
		initial := <-store
		if initial.Code != trans.From {
			t.Fatalf("%d Initial state %q not set. Found: %q", i, trans.From, initial.Code)
		}

		// The Fault state transitions itself to either sleeping or failed
		if trans.From != statemachine.Fault {
			// Apply the Event to transition to the To state
			msg := statemachine.Message{Code: trans.Event}

			// Sleep messages need extra state
			if trans.Event == statemachine.Sleep {
				until := time.Now().Add(10 * time.Millisecond)
				msg.Until = &until
			}
			if err := cmdr.Send("test", statemachine.Message{Code: trans.Event}); err != nil {
				t.Fatalf("Error sending message %s: %v", trans.Event, err)
			}
		}
		newstate := <-store
		if trans.From == statemachine.Fault && trans.To == statemachine.Failed {
			// continue on as this transition relies on state this test doesn't exercise
			continue
		}
		if newstate.Code != trans.To {
			t.Fatalf("%d Expected %q but found %q for transition %s", i, trans.To, newstate.Code, trans)
		}
	}
}

func TestCheckpointRelease(t *testing.T) {
	t.Parallel()
	ss := embedded.NewStateStore()
	ss.Store("test1", &statemachine.State{Code: statemachine.Runnable})
	cmdr := embedded.NewCommander()
	cmdlistener := cmdr.NewListener("test1")
	sm := statemachine.New("test1", testhandler, ss, cmdlistener, nil)
	done := make(chan bool)
	go func() { done <- sm.Run() }()
	// Should just cause statemachine to loop
	if err := cmdr.Send("test1", statemachine.Message{Code: statemachine.Checkpoint}); err != nil {
		t.Fatalf("Error sending checkpoint: %v", err)
	}
	select {
	case <-done:
		t.Fatalf("Checkpoint command should not have caused statemachine to exit.")
	case <-time.After(100 * time.Millisecond):
	}

	// Should cause the statemachine to exit
	if err := cmdr.Send("test1", statemachine.Message{Code: statemachine.Release}); err != nil {
		t.Fatalf("Error sending release: %v", err)
	}
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

func TestSleep(t *testing.T) {
	t.Parallel()

	ss := embedded.NewStateStore().(*embedded.StateStore)
	ss.Store("sleep-test", &statemachine.State{Code: statemachine.Runnable})
	<-ss.Stored
	cmdr := embedded.NewCommander()
	cmdl := cmdr.NewListener("sleep-test")
	sm := statemachine.New("sleep-test", testhandler, ss, cmdl, nil)
	done := make(chan bool)
	go func() { done <- sm.Run() }()

	{
		// Put to sleep forever
		until := time.Now().Add(9001 * time.Hour)
		if err := cmdr.Send("sleep-test", statemachine.Message{Code: statemachine.Sleep, Until: &until}); err != nil {
			t.Fatalf("Error sending sleep: %v", err)
		}

		newstate := <-ss.Stored
		if newstate.State.Code != statemachine.Sleeping || !newstate.State.Until.Equal(until) {
			t.Fatalf("Expected task to store state Sleeping, but stored: %s", newstate)
		}
	}

	// Make sure it stays sleeping for at least a bit
	select {
	case newstate := <-ss.Stored:
		t.Fatalf("Expected task to stay asleep forever but transitioned to: %s", newstate)
	case <-time.After(100 * time.Millisecond):
	}

	// Override current sleep with a shorter one
	dur := 1 * time.Second
	start := time.Now()
	until := start.Add(dur)
	if err := cmdr.Send("sleep-test", statemachine.Message{Code: statemachine.Sleep, Until: &until}); err != nil {
		t.Fatalf("Error sending sleep: %v", err)
	}

	newstate := <-ss.Stored
	if newstate.State.Code != statemachine.Sleeping || !newstate.State.Until.Equal(until) {
		t.Fatalf("Expected task to store state Sleeping, but stored: %s", newstate)
	}

	// Make sure it transitions to Runnable after sleep has elapsed
	newstate = <-ss.Stored
	transitioned := time.Now()
	if newstate.State.Code != statemachine.Runnable || newstate.State.Until != nil {
		t.Fatalf("Expected task to be runnable without an Until time but found: %s", newstate.State)
	}
	elapsed := transitioned.Sub(start)
	if transitioned.Sub(start) < dur {
		t.Fatalf("Expected task to sleep for %s but slept for %s", dur, elapsed)
	}
	t.Logf("Statemachine latency: %s", elapsed-dur)
}
