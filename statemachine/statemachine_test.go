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

// setup a task with the specified task ID in a stateful handler and run it.
func setup(t *testing.T, tid string) (*embedded.StateStore, statemachine.Commander, metafora.Handler, chan bool) {
	t.Parallel()
	ss := embedded.NewStateStore().(*embedded.StateStore)
	ss.Store(tid, &statemachine.State{Code: statemachine.Runnable})
	<-ss.Stored // pop initial state out
	cmdr := embedded.NewCommander()
	cmdlistener := cmdr.NewListener(tid)
	sm := statemachine.New(tid, testhandler, ss, cmdlistener, nil)
	done := make(chan bool)
	go func() { done <- sm.Run() }()
	return ss, cmdr, sm, done
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
	ss, cmdr, _, done := setup(t, "test1")

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
	ss, cmdr, _, _ := setup(t, "sleep-test")

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

func TestTerminal(t *testing.T) {
	ss, cmdr, sm, done := setup(t, "terminal-test")

	// Kill the task
	if err := cmdr.Send("terminal-test", statemachine.Message{Code: statemachine.Kill}); err != nil {
		t.Fatalf("Error sending kill command: %v", err)
	}

	// Task should be killed and done (unscheduled)
	newstate := <-ss.Stored
	if newstate.State.Code != statemachine.Killed {
		t.Fatalf("Expected task to be killed but found: %s", newstate.State)
	}
	if !(<-done) {
		t.Fatal("Expected task to be done.")
	}
	if state, err := ss.Load("terminal-test"); err != nil || state.Code != statemachine.Killed {
		t.Fatalf("Failed to load expected killed state for task: state=%s err=%v", state, err)
	}

	// Task should just die again if we try to reschedule it
	go func() { done <- sm.Run() }()
	select {
	case newstate := <-ss.Stored:
		t.Fatalf("Re-running a terminated task should *not* store state, but it stored: %v", newstate.State)
	case <-time.After(100 * time.Millisecond):
		// State shouldn't even be stored since it's not being changed and terminal
		// states should be immutable
	}

	if !(<-done) {
		t.Fatal("Expected task to be done.")
	}
}

func TestPause(t *testing.T) {
	ss, cmdr, sm, done := setup(t, "test-pause")

	pause := func() {
		if err := cmdr.Send("test-pause", statemachine.Message{Code: statemachine.Pause}); err != nil {
			t.Fatalf("Error sending pause command to test-pause: %v", err)
		}
		newstate := <-ss.Stored
		if newstate.State.Code != statemachine.Paused {
			t.Fatalf("Expected paused state but found: %s", newstate.State)
		}
		if state, err := ss.Load("test-pause"); err != nil || state.Code != statemachine.Paused {
			t.Fatalf("Failed to load expected pause state for task: state=%s err=%v", state, err)
		}

		// Task should not be Done; pausing doesn't exit the statemachine
		select {
		case <-done:
			t.Fatal("Task exited unexpectedly.")
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Pause the work
	pause()

	// Should be able to resume paused work
	if err := cmdr.Send("test-pause", statemachine.Message{Code: statemachine.Run}); err != nil {
		t.Fatalf("Error sending run command to test-pause: %v", err)
	}
	newstate := <-ss.Stored
	if newstate.State.Code != statemachine.Runnable {
		t.Fatalf("Expected runnable state but found: %s", newstate.State)
	}
	if state, err := ss.Load("test-pause"); err != nil || state.Code != statemachine.Runnable {
		t.Fatalf("Failed to load expected runnable state for task: state=%s err=%v", state, err)
	}

	// Re-pause the work
	pause()

	// Pausing paused work is silly but fine
	pause()

	// Releasing paused work should make it exit but leave it in the paused state
	sm.Stop()
	newstate = <-ss.Stored
	if newstate.State.Code != statemachine.Paused {
		t.Fatalf("Releasing should not have changed paused state but stored: %s", newstate.State)
	}
	select {
	case d := <-done:
		if d {
			t.Fatal("Releasing task should not have marked it as done.")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Releasing paused task should have exited the statemachine, but didn't.")
	}

	// Ensure task is stored with the paused state
	if state, err := ss.Load("test-pause"); err != nil || state.Code != statemachine.Paused {
		t.Fatalf("Failed to load expected paused state for task: state=%s err=%v", state, err)
	}
}
