package embedded_test

import (
	"testing"
	"time"

	"github.com/lytics/metafora/embedded"
	"github.com/lytics/metafora/statemachine"
)

func TestEmbeddedCommander(t *testing.T) {
	t.Parallel()
	cmdr := embedded.NewCommander()
	cl1 := cmdr.NewListener("task1")
	cl2 := cmdr.NewListener("task2")

	if err := cmdr.Send("task1", statemachine.Message{Code: statemachine.Run}); err != nil {
		t.Fatalf("Error sending message to task1: %v", err)
	}
	if err := cmdr.Send("task2", statemachine.Message{Code: statemachine.Release}); err != nil {
		t.Fatalf("Error sending message to task2: %v", err)
	}
	if err := cmdr.Send("invalid-task", statemachine.Message{Code: statemachine.Pause}); err == nil {
		t.Fatal("Expected an error when sending to an invalid task, but didn't receive one.")
	}

	msg2 := <-cl2.Receive()
	if msg2.Code != statemachine.Release {
		t.Fatalf("listener2 expected a Run message but received: %#v", msg2)
	}
	msg1 := <-cl1.Receive()
	if msg1.Code != statemachine.Run {
		t.Fatalf("listener1 expected a Run message but received: %#v", msg1)
	}

	// Stop listeners and make sure nothing works (but doesn't panic)
	cl1.Stop()
	cl2.Stop()

	select {
	case <-cl1.Receive():
		t.Fatal("expected listener1 to be close but it still received a message!")
	case <-cl2.Receive():
		t.Fatal("expected listener2 to be close but it still received a message!")
	case <-time.After(50 * time.Millisecond):
	}
}
