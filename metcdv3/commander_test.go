package metcdv3

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

func TestCommandListener(t *testing.T) {
	t.Parallel()

	etcdv3c, _, conf := setupEtcd(t)
	kvc := etcdv3.NewKV(etcdv3c)

	namespace := "/cltest"
	conf.Namespace = namespace
	_, _ = kvc.Delete(context.Background(), namespace, etcdv3.WithPrefix())

	task := metafora.NewTask("testtask")
	_, err := kvc.Put(context.Background(), path.Join(conf.Namespace, TasksPath, task.ID(), OwnerPath), fmt.Sprintf(`{"node":"%s"}`, conf.Name))
	if err != nil {
		t.Fatalf("Error creating fake claim: %v", err)
	}

	cmdr := NewCommander(namespace, etcdv3c)

	// Only the last command should be received once the listener is started
	_ = cmdr.Send(task.ID(), statemachine.PauseMessage())
	_ = cmdr.Send(task.ID(), statemachine.KillMessage())

	cl := NewCommandListener(conf, task, etcdv3c)
	defer cl.Stop()

	// Ensure last command was received
	select {
	case cmd := <-cl.Receive():
		if cmd.Code != statemachine.Kill {
			t.Fatalf("Expected Kill message, received %v", cmd)
		}
	case <-time.After(10 * time.Second):
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
	_ = cmdr.Send(task.ID(), statemachine.RunMessage())
	select {
	case cmd := <-cl.Receive():
		t.Fatalf("Unexpected command received: %v", cmd)
	case <-time.After(300 * time.Millisecond):
		// Ok
	}
}

// A listener on a node that doesn't hold the claim must neither consume the
// command nor delete it, so the node that does hold the claim still gets it.
func TestCommandListenerLeavesCommandsForTheOwner(t *testing.T) {
	t.Parallel()

	etcdv3c, _, conf := setupEtcd(t)
	kvc := etcdv3.NewKV(etcdv3c)

	namespace := "/clnotownedtest"
	conf.Namespace = namespace
	_, _ = kvc.Delete(context.Background(), namespace, etcdv3.WithPrefix())

	task := metafora.NewTask("testtask")
	cmdpath := path.Join(namespace, TasksPath, task.ID(), CommandsPath)

	// Claimed by somebody else.
	_, err := kvc.Put(context.Background(),
		path.Join(namespace, TasksPath, task.ID(), OwnerPath), `{"node":"someothernode"}`)
	if err != nil {
		t.Fatalf("Error creating fake claim: %v", err)
	}

	if err := NewCommander(namespace, etcdv3c).Send(task.ID(), statemachine.RunMessage()); err != nil {
		t.Fatalf("Error sending command: %v", err)
	}

	cl := NewCommandListener(conf, task, etcdv3c)
	defer cl.Stop()

	select {
	case cmd := <-cl.Receive():
		t.Fatalf("Received a command for a task this node doesn't own: %v", cmd)
	case <-time.After(time.Second):
		// Ok!
	}

	// The command must still be there for the real owner.
	res, err := kvc.Get(context.Background(), cmdpath)
	if err != nil {
		t.Fatalf("Error reading command back: %v", err)
	}
	if res.Count != 1 {
		t.Fatalf("Expected the command to survive, found %d keys at %s", res.Count, cmdpath)
	}
}
