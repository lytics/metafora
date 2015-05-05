package m_etcd_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
	"github.com/lytics/metafora/m_etcd/testutil"
	"github.com/lytics/metafora/statemachine"
)

// TestNew is an integration test for m_etcd's New function.
func TestNew(t *testing.T) {
	t.Parallel()
	etcdc := testutil.NewEtcdClient(t)

	const recursive = true
	etcdc.Delete("testnew1", recursive)
	etcdc.Delete("testnew2", recursive)

	// Start 2 simple consumers in different namespaces
	h1 := func(_ string, cmds <-chan statemachine.Message) statemachine.Message {
		return <-cmds
	}
	coord1, hf1, bal1 := m_etcd.New("node1", "testnew1", etcdc, h1)
	cons1, err := metafora.NewConsumer(coord1, hf1, bal1)
	if err != nil {
		t.Fatalf("Error creating consumer 1: %v", err)
	}
	done1 := make(chan bool)
	go func() {
		defer close(done1)
		cons1.Run()
	}()

	h2 := func(_ string, cmds <-chan statemachine.Message) statemachine.Message {
		return <-cmds
	}
	coord2, hf2, bal2 := m_etcd.New("node1", "testnew2", etcdc, h2)
	cons2, err := metafora.NewConsumer(coord2, hf2, bal2)
	if err != nil {
		t.Fatalf("Error creating consumer 1: %v", err)
	}
	done2 := make(chan bool)
	go func() {
		defer close(done2)
		cons2.Run()
	}()

	// Create clients and start some tests
	cli1 := m_etcd.NewClient("testnew1", etcdc)
	cli2 := m_etcd.NewClient("testnew2", etcdc)

	if err := cli1.SubmitTask("task1"); err != nil {
		t.Fatalf("Error submitting task1: %v", err)
	}
	if err := cli2.SubmitTask("task2"); err != nil {
		t.Fatalf("Error submitting task2: %v", err)
	}

	// Give consumers a bit to pick up tasks
	time.Sleep(250 * time.Millisecond)

	{
		tasks := cons1.Tasks()
		if len(tasks) != 1 || tasks[0].ID() != "task1" || !tasks[0].Stopped().IsZero() {
			buf, _ := json.Marshal(tasks)
			t.Fatalf("Expected task1 to be running but found: %s", string(buf))
		}
	}

	{
		tasks := cons2.Tasks()
		if len(tasks) != 1 || tasks[0].ID() != "task2" || !tasks[0].Stopped().IsZero() {
			buf, _ := json.Marshal(tasks)
			t.Fatalf("Expected task2 to be running but found: %s", string(buf))
		}
	}

	// Kill task1
	cmdr := m_etcd.NewCommander("testnew1", etcdc)
	if err := cmdr.Send("task1", statemachine.Message{Code: statemachine.Kill}); err != nil {
		t.Fatalf("Error sending kill to task1: %v", err)
	}
	time.Sleep(250 * time.Millisecond)

	{
		tasks := cons1.Tasks()
		if len(tasks) != 0 {
			buf, _ := json.Marshal(tasks)
			t.Fatalf("Expected no tasks but found: %s", string(buf))
		}
	}

	{
		tasks := cons2.Tasks()
		if len(tasks) != 1 || tasks[0].ID() != "task2" || !tasks[0].Stopped().IsZero() {
			buf, _ := json.Marshal(tasks)
			t.Fatalf("Expected task2 to be running but found: %s", string(buf))
		}
	}

	// Shutdown both consumers
	cons1.Shutdown()
	cons2.Shutdown()
	<-done1
	<-done2
}
