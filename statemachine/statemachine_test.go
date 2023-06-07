package statemachine_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/embedded"
	. "github.com/lytics/metafora/statemachine"
)

func testhandler(task metafora.Task, cmds <-chan *Message) *Message {
	metafora.Debugf("Starting %s", task.ID())
	m := <-cmds
	metafora.Debugf("%s recvd %s", task.ID(), m.Code)
	return m
}

type testStore struct {
	initial *State
	out     chan<- *State
}

func (s testStore) Load(metafora.Task) (*State, error) {
	s.out <- s.initial
	return s.initial, nil
}
func (s testStore) Store(task metafora.Task, newstate *State) error {
	metafora.Debugf("%s storing %s", task.ID(), newstate.Code)
	s.out <- newstate
	return nil
}

// setup a task with the specified task ID in a stateful handler and run it.
func setup(t *testing.T, tid string) (*embedded.StateStore, Commander, metafora.Handler, chan bool) {
	t.Parallel()
	ss := embedded.NewStateStore().(*embedded.StateStore)
	_ = ss.Store(task(tid), &State{Code: Runnable})
	<-ss.Stored // pop initial state out
	cmdr := embedded.NewCommander()
	cmdlistener := cmdr.NewListener(tid)
	sm := New(task(tid), testhandler, ss, cmdlistener, nil)
	done := make(chan bool)
	go func() { done <- sm.Run() }()
	return ss, cmdr, sm, done
}

// FIXME leaks goroutines
func TestRules(t *testing.T) {
	t.Parallel()
	for i, trans := range Rules {
		metafora.Debugf("Trying %s", trans)
		cmdr := embedded.NewCommander()
		cmdlistener := cmdr.NewListener("test")
		store := make(chan *State)

		state := &State{Code: trans.From}

		// Sleeping state needs extra Until state
		if trans.From == Sleeping {
			until := time.Now().Add(100 * time.Millisecond)
			state.Until = &until
		}

		ts := testStore{initial: state, out: store}

		// Create a new statemachine that starts from the From state
		sm := New(task("test"), testhandler, ts, cmdlistener, nil)
		go sm.Run()
		initial := <-store
		if initial.Code != trans.From {
			t.Fatalf("%d Initial state %q not set. Found: %q", i, trans.From, initial.Code)
		}

		// The Fault state transitions itself to either sleeping or failed
		if trans.From != Fault {
			// Apply the Event to transition to the To state
			msg := &Message{Code: trans.Event}

			// Sleep messages need extra state
			if trans.Event == Sleep {
				until := time.Now().Add(10 * time.Millisecond)
				msg.Until = &until
			}
			if trans.Event == Error {
				msg.Err = errors.New("test")
			}
			if err := cmdr.Send("test", msg); err != nil {
				t.Fatalf("Error sending message %s: %v", trans.Event, err)
			}
		}
		newstate := <-store
		if trans.From == Fault && trans.To == Failed {
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
	if err := cmdr.Send("test1", CheckpointMessage()); err != nil {
		t.Fatalf("Error sending checkpoint: %v", err)
	}
	select {
	case <-done:
		t.Fatalf("Checkpoint command should not have caused statemachine to exit.")
	case <-time.After(100 * time.Millisecond):
	}

	// Should cause the statemachine to exit
	if err := cmdr.Send("test1", ReleaseMessage()); err != nil {
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
	state, err := ss.Load(task("test1"))
	if err != nil {
		t.Fatal(err)
	}
	if state.Code != Runnable {
		t.Fatalf("Expected released task to be runnable but found state %q", state.Code)
	}
}

func TestSleep(t *testing.T) {
	ss, cmdr, _, _ := setup(t, "sleep-test")

	{
		// Put to sleep forever
		until := time.Now().Add(9001 * time.Hour)
		if err := cmdr.Send("sleep-test", SleepMessage(until)); err != nil {
			t.Fatalf("Error sending sleep: %v", err)
		}

		newstate := <-ss.Stored
		if newstate.State.Code != Sleeping || !newstate.State.Until.Equal(until) {
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
	if err := cmdr.Send("sleep-test", SleepMessage(until)); err != nil {
		t.Fatalf("Error sending sleep: %v", err)
	}

	newstate := <-ss.Stored
	if newstate.State.Code != Sleeping || !newstate.State.Until.Equal(until) {
		t.Fatalf("Expected task to store state Sleeping, but stored: %s", newstate)
	}

	// Make sure it transitions to Runnable after sleep has elapsed
	newstate = <-ss.Stored
	transitioned := time.Now()
	if newstate.State.Code != Runnable || newstate.State.Until != nil {
		t.Fatalf("Expected task to be runnable without an Until time but found: %s", newstate.State)
	}
	elapsed := transitioned.Sub(start)
	if transitioned.Sub(start) < dur {
		t.Fatalf("Expected task to sleep for %s but slept for %s", dur, elapsed)
	}
	t.Logf("Statemachine latency: %s", elapsed-dur)
}

func TestSleepRelease(t *testing.T) {
	ss, cmdr, _, returned := setup(t, "sleep-test")

	until := time.Now().Add(9001 * time.Hour)
	{
		// Put to sleep forever
		if err := cmdr.Send("sleep-test", SleepMessage(until)); err != nil {
			t.Fatalf("Error sending sleep: %v", err)
		}

		newstate := <-ss.Stored
		if newstate.State.Code != Sleeping || !newstate.State.Until.Equal(until) {
			t.Fatalf("Expected task to store state Sleeping, but stored: %s", newstate)
		}
	}

	{
		// Releasing should maintain sleep state but exit
		if err := cmdr.Send("sleep-test", ReleaseMessage()); err != nil {
			t.Fatalf("Error sending release: %v", err)
		}
		newstate := <-ss.Stored
		if newstate.State.Code != Sleeping || newstate.State.Until == nil || !newstate.State.Until.Equal(until) {
			t.Fatalf("Releasing unexpectedly changed state: %s != Sleeping || %v != %s", newstate.State.Code, newstate.State.Until, until)
		}
		if done := <-returned; done {
			t.Fatal("Releasing should not have returned done.")
		}
	}
}

func TestTerminal(t *testing.T) {
	ss, cmdr, sm, done := setup(t, "terminal-test")

	// Kill the task
	if err := cmdr.Send("terminal-test", &Message{Code: Kill}); err != nil {
		t.Fatalf("Error sending kill command: %v", err)
	}

	// Task should be killed and done (unscheduled)
	newstate := <-ss.Stored
	if newstate.State.Code != Killed {
		t.Fatalf("Expected task to be killed but found: %s", newstate.State)
	}
	if !(<-done) {
		t.Fatal("Expected task to be done.")
	}
	if state, err := ss.Load(task("terminal-test")); err != nil || state.Code != Killed {
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
		if err := cmdr.Send("test-pause", PauseMessage()); err != nil {
			t.Fatalf("Error sending pause command to test-pause: %v", err)
		}
		newstate := <-ss.Stored
		if newstate.State.Code != Paused {
			t.Fatalf("Expected paused state but found: %s", newstate.State)
		}
		if state, err := ss.Load(task("test-pause")); err != nil || state.Code != Paused {
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
	if err := cmdr.Send("test-pause", RunMessage()); err != nil {
		t.Fatalf("Error sending run command to test-pause: %v", err)
	}
	newstate := <-ss.Stored
	if newstate.State.Code != Runnable {
		t.Fatalf("Expected runnable state but found: %s", newstate.State)
	}
	if state, err := ss.Load(task("test-pause")); err != nil || state.Code != Runnable {
		t.Fatalf("Failed to load expected runnable state for task: state=%s err=%v", state, err)
	}

	// Re-pause the work
	pause()

	// Pausing paused work is silly but fine
	pause()

	// Releasing paused work should make it exit but leave it in the paused state
	sm.Stop()
	newstate = <-ss.Stored
	if newstate.State.Code != Paused {
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
	if state, err := ss.Load(task("test-pause")); err != nil || state.Code != Paused {
		t.Fatalf("Failed to load expected paused state for task: state=%s err=%v", state, err)
	}
}

func TestMessageValid(t *testing.T) {
	t.Parallel()
	until := time.Now()
	validmsgs := []Message{
		{Code: Run},
		{Code: Sleep, Until: &until},
		{Code: Pause},
		{Code: Kill},
		{Code: Error, Err: errors.New("test")},
		{Code: Complete},
		{Code: Checkpoint},
		{Code: Release},
	}
	for _, m := range validmsgs {
		if !m.Valid() {
			t.Errorf("Expected %s to be valid.", m)
		}
	}

	invalidmsgs := []Message{
		{},
		{Code: Sleep},
		{Code: Error},
	}
	for _, m := range invalidmsgs {
		if m.Valid() {
			t.Errorf("Expected %s to be invalid.", m)
		}
	}
}
