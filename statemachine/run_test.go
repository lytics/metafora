package statemachine

import (
	"testing"
	"time"

	"github.com/lytics/metafora"
)

type task string

func (t task) ID() string { return string(t) }

// TestCommandBlackhole is meant to demonstrate what happens if a
// StatefulHandler implementation receives commands in a goroutine that lives
// past the SatefulHandler func exiting. This is a very easy bug to write, so
// defensive code was added to prevent the leaked goroutine from "stealing"
// commands meant for other states (Paused or Sleeping being the two states
// that absolutely need to accept commands).
//
// This test breaking isn't necessarily the sign of a bug. It may just mean
// we've decided to remove the defensive code protecting against such errors in
// which case this test should be removed as well.
func TestCommandBlackhole(t *testing.T) {
	t.Parallel()
	stop := make(chan bool)
	rdy := make(chan int, 1)
	defer close(stop)

	f := func(_ metafora.Task, c <-chan *Message) *Message {
		go func() {
			rdy <- 1
			select {
			case <-c:
				t.Log("Intercepted!")
			case <-stop:
				return
			}
		}()
		return nil
	}
	cmds := make(chan *Message)

	// Ignore the return message, the point is to make sure it doesn't intercept
	// further commands.
	run(f, task("test-task"), cmds)
	<-rdy

	go func() { cmds <- RunMessage() }()

	select {
	case <-cmds:
		// Yay! command wasn't intercepted by leaked goroutine!
	case <-time.After(time.Second):
		t.Fatalf("Command was intercepted by leaked goroutine.")
	}
}
