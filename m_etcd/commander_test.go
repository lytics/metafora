package m_etcd_test

import (
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
	"github.com/lytics/metafora/m_etcd/testutil"
	"github.com/lytics/metafora/statemachine"
)

func TestCommandListener(t *testing.T) {
	t.Parallel()

	// etcd clients are not safe for concurrent use, so create one for each
	// component
	cmdrclient, _ := testutil.NewEtcdClient(t)
	clclient, _ := testutil.NewEtcdClient(t)

	const recursive = true
	namespace := "cltest"
	cmdrclient.Delete("/"+namespace, recursive)

	task := metafora.NewTask("testtask")

	cmdr := m_etcd.NewCommander(namespace, cmdrclient)

	// Only the last command should be received once the listener is started
	cmdr.Send(task.ID(), statemachine.PauseMessage())
	cmdr.Send(task.ID(), statemachine.KillMessage())

	cl := m_etcd.NewCommandListener(task, namespace, clclient)
	defer cl.Stop()

	// Ensure last command was received
	select {
	case cmd := <-cl.Receive():
		if cmd.Code != statemachine.Kill {
			t.Fatalf("Expected Kill message, received %v", cmd)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CommandListener took too long to receive message")
	}

	// Ensure only one command was received
	select {
	case cmd := <-cl.Receive():
		t.Fatalf("Unexpected command received: %v", cmd)
	case <-time.After(300 * time.Millisecond):
		// Ok!
	}

	cl.Stop()

	// Stop doesn't block until watching loop exits, so wait briefly
	time.Sleep(10 * time.Millisecond)

	// Ensure receiving after Stopping never succeeds
	cmdr.Send(task.ID(), statemachine.RunMessage())
	select {
	case cmd := <-cl.Receive():
		t.Fatalf("Unexpected command received: %v", cmd)
	case <-time.After(300 * time.Millisecond):
		// Ok
	}
}
