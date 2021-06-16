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
	kvc.Delete(context.Background(), namespace, etcdv3.WithPrefix())

	task := metafora.NewTask("testtask")
	_, err := kvc.Put(context.Background(), path.Join(conf.Namespace, TasksPath, task.ID(), OwnerPath), fmt.Sprintf(`{"node":"%s"}`, conf.Name))
	if err != nil {
		t.Fatalf("Error creating fake claim: %v", err)
	}

	cmdr := NewCommander(namespace, etcdv3c)

	// Only the last command should be received once the listener is started
	cmdr.Send(task.ID(), statemachine.PauseMessage())
	cmdr.Send(task.ID(), statemachine.KillMessage())

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
	cmdr.Send(task.ID(), statemachine.RunMessage())
	select {
	case cmd := <-cl.Receive():
		t.Fatalf("Unexpected command received: %v", cmd)
	case <-time.After(300 * time.Millisecond):
		// Ok
	}
}
