package metcdv3_test

import (
	"context"
	"errors"
	"testing"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/metcdv3"
	"github.com/lytics/metafora/metcdv3/testutil"
	"github.com/lytics/metafora/statemachine"
)

const recursive = true

// TestSleepTest is an integration test for all of m_etcd's components.
//
func TestSleepTest(t *testing.T) {
	etcdv3c := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(etcdv3c)
	t.Parallel()
	const namespace = "/sleeptest-metafora"
	const sleepingtasks = "sleeping-task1"

	kvc.Delete(context.Background(), namespace, etcdv3.WithPrefix())

	holdtask := make(chan bool)
	h := func(task metafora.Task, cmds <-chan *statemachine.Message) *statemachine.Message {

		if task.ID() == sleepingtasks {
			sleeptil := 5 * time.Second
			nextstarttime := (time.Now().Add(sleeptil))
			t.Logf("sleeping task:%v sleepfor:%v", task, nextstarttime)
			<-holdtask
			return statemachine.SleepMessage(nextstarttime)
		}

		cmd := <-cmds
		t.Logf("non sleeping task:%v", task)

		return cmd
	}

	newC := func(name, ns string) *metafora.Consumer {
		conf := metcdv3.NewConfig(name, ns)
		coord, hf, bal := metcdv3.New(conf, etcdv3c, h)
		cons, err := metafora.NewConsumer(coord, hf, bal)
		if err != nil {
			t.Fatalf("Error creating consumer %s:%s: %v", ns, name, err)
		}
		go func() {
			cons.Run()
			t.Logf("Consumer:%s exited.", name)
		}()
		return cons
	}

	assertRunning := func(tid string, cons ...*metafora.Consumer) {
		found := false
		for _, c := range cons {
			tasks := c.Tasks()
			if len(tasks) > 0 && found {
				t.Fatal("Task already found running but another task is running on a different consumer")
			}
			if len(tasks) > 1 {
				t.Fatalf("Expected at most 1 task, but found: %d", len(tasks))
			}
			if len(tasks) == 1 && tasks[0].Task().ID() == tid {
				found = true
			}
		}
		if !found {
			t.Fatalf("Could not find task=%q", tid)
		}
	}

	// Start 2 consumers
	cons1 := newC("node1", namespace)
	cons2 := newC("node2", namespace)

	// Create clients and start some tests
	cliA := metcdv3.NewClient(namespace, etcdv3c)

	if err := cliA.SubmitTask(metcdv3.DefaultTaskFunc(sleepingtasks, "")); err != nil {
		t.Fatalf("Error submitting task1 to a: %v", err)
	}

	// Give consumers a bit to pick up tasks
	time.Sleep(500 * time.Millisecond)

	assertRunning(sleepingtasks, cons1, cons2)

	holdtask <- true
	// Give consumers a bit to pick up tasks
	time.Sleep(500 * time.Millisecond)

	assertRunning(sleepingtasks, cons1, cons2) // not sure if this should be true or false.

	wait1 := make(chan bool)
	go func() {
		defer close(wait1)
		// Shutdown
		cons1.Shutdown()
		cons2.Shutdown()
	}()

	timeout := time.NewTimer(5 * time.Second)
	select {
	case <-wait1:
	case <-timeout.C:
		t.Fatalf("failed waiting for shutdown")
	}

	//	make sure all tasks are released
	for _, c := range []*metafora.Consumer{cons1, cons2} {
		tasks := c.Tasks()
		for _, work := range tasks {
			t.Fatalf("work id %v is still running", work)
		}
	}
}

// TestAll is an integration test for all of m_etcd's components.
//
// While huge integration tests like this are rarely desirable as they can be
// overly fragile and complex, I found myself manually repeating the tests I've
// automated here over and over. This is far more reliable than expecting
// developers to do adhoc testing of all of the m_etcd package's features.
func TestAll(t *testing.T) {
	etcdv3c := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(etcdv3c)
	t.Parallel()

	c := context.Background()
	kvc.Delete(c, "/test-a", etcdv3.WithPrefix())
	kvc.Delete(c, "/test-b", etcdv3.WithPrefix())

	h := func(task metafora.Task, cmds <-chan *statemachine.Message) *statemachine.Message {
		cmd := <-cmds
		if task.ID() == "error-test" {
			return statemachine.ErrorMessage(errors.New("error-test"))
		}
		return cmd
	}

	newC := func(name, ns string) *metafora.Consumer {
		conf := metcdv3.NewConfig(name, ns)
		conf.Name = name
		coord, hf, bal := metcdv3.New(conf, etcdv3c, h)
		cons, err := metafora.NewConsumer(coord, hf, bal)
		if err != nil {
			t.Fatalf("Error creating consumer %s:%s: %v", ns, name, err)
		}
		go cons.Run()
		return cons
	}
	// Start 4 consumers, 2 per namespace
	cons1a := newC("node1", "/test-a")
	cons2a := newC("node2", "/test-a")
	cons1b := newC("node1", "/test-b")
	cons2b := newC("node2", "/test-b")

	// Create clients and start some tests
	cliA := metcdv3.NewClient("/test-a", etcdv3c)
	cliB := metcdv3.NewClient("/test-b", etcdv3c)

	if err := cliA.SubmitTask(metcdv3.DefaultTaskFunc("task1", "")); err != nil {
		t.Fatalf("Error submitting task1 to a: %v", err)
	}
	if err := cliB.SubmitTask(metcdv3.DefaultTaskFunc("task1", "")); err != nil {
		t.Fatalf("Error submitting task1 to b: %v", err)
	}

	// Give consumers a bit to pick up tasks
	time.Sleep(250 * time.Millisecond)

	assertRunning := func(tid string, cons ...*metafora.Consumer) {
		found := false
		for _, c := range cons {
			tasks := c.Tasks()
			if len(tasks) > 0 && found {
				t.Fatal("Task already found running but another task is running on a different consumer")
			}
			if len(tasks) > 1 {
				t.Fatalf("Expected at most 1 task, but found: %d", len(tasks))
			}
			if len(tasks) == 1 && tasks[0].Task().ID() == tid {
				found = true
			}
		}
		if !found {
			t.Fatalf("Could not find task=%q", tid)
		}
	}

	assertRunning("task1", cons1a, cons2a)
	assertRunning("task1", cons1b, cons2b)

	// Kill task1 in A
	{
		cmdr := metcdv3.NewCommander("/test-a", etcdv3c)
		if err := cmdr.Send("task1", statemachine.KillMessage()); err != nil {
			t.Fatalf("Error sending kill to task1: %v", err)
		}
		time.Sleep(500 * time.Millisecond)

		for _, c := range []*metafora.Consumer{cons1a, cons2a} {
			tasks := c.Tasks()
			if len(tasks) != 0 {
				t.Fatalf("Expected no tasks but found: %d", len(tasks))
			}
		}
	}

	// Submit a bunch of tasks to A
	{
		tasks := []string{"task2", "task3", "task4", "task5", "task6", "task7"}
		for _, tid := range tasks {
			if err := cliA.SubmitTask(metcdv3.DefaultTaskFunc(tid, "")); err != nil {
				t.Fatalf("Error submitting task=%q to A: %v", tid, err)
			}
		}

		// Give them time to start
		time.Sleep(800 * time.Millisecond)

		// Ensure they're balanced
		if err := cliA.SubmitCommand("node1", metafora.CommandBalance()); err != nil {
			t.Fatalf("Error submitting balance command to cons1a: %v", err)
		}
		time.Sleep(800 * time.Millisecond)
		if err := cliA.SubmitCommand("node2", metafora.CommandBalance()); err != nil {
			t.Fatalf("Error submitting balance command to cons1a: %v", err)
		}

		a1tasks := cons1a.Tasks()
		a2tasks := cons2a.Tasks()
		for _, task := range a1tasks {
			metafora.Debug("A1: ", task.Task(), " - ", task.Stopped().IsZero())
		}
		for _, task := range a2tasks {
			metafora.Debug("A2: ", task.Task(), " - ", task.Stopped().IsZero())
		}
		time.Sleep(800 * time.Millisecond)

		a1tasks = cons1a.Tasks()
		a2tasks = cons2a.Tasks()
		if len(a1tasks) < 2 || len(a1tasks) > 4 || len(a2tasks) < 2 || len(a2tasks) > 4 {
			t.Fatalf("Namespace A isn't fairly balanced: node1: %d; node2: %d", len(a1tasks), len(a2tasks))
		}

		// Shutting down a consumer should migrate all tasks to the other
		cons1a.Shutdown()
		time.Sleep(800 * time.Millisecond)

		a2tasks = cons2a.Tasks()
		if len(a2tasks) != len(tasks) {
			t.Fatalf("Consumer 2a should have received all %d tasks but only has %d.", len(tasks), len(a2tasks))
		}
	}

	// Use Namespace B to check Error state handling
	{
		tasks := []string{"task8", "error-test"}
		for _, tid := range tasks {
			if err := cliB.SubmitTask(metcdv3.DefaultTaskFunc(tid, "")); err != nil {
				t.Fatalf("Error submitting task=%q to B: %v", tid, err)
			}
		}

		// Give them time to start
		time.Sleep(time.Second)

		n := len(cons1b.Tasks()) + len(cons2b.Tasks())
		if n != 3 {
			t.Fatalf("Expected B to be running 3 tasks but found %d", n)
		}

		// Resuming error-test 8*2 times should cause it to be failed
		cmdr := metcdv3.NewCommander("/test-b", etcdv3c)
		for i := 0; i < statemachine.DefaultErrMax*2; i++ {
			if err := cmdr.Send("error-test", statemachine.RunMessage()); err != nil {
				t.Fatalf("Unexpected error resuming error-test in B: %v", err)
			}
			time.Sleep(500 * time.Millisecond)
		}

		n = len(cons1b.Tasks()) + len(cons2b.Tasks())
		if n != 2 {
			t.Fatalf("Expected B to be running 2 tasks but found %d", n)
		}

		// Resubmitting a failed task shouldn't error but also shouldn't run.
		if err := cliB.SubmitTask(metcdv3.DefaultTaskFunc("error-test", "")); err != nil {
			t.Fatalf("Error resubmitting error-test task to B: %v", err)
		}

		// Give the statemachine a moment to load the initial state and exit
		time.Sleep(time.Second)

		n = len(cons1b.Tasks()) + len(cons2b.Tasks())
		if n != 2 {
			t.Fatalf("Expected B to be running 2 tasks but found %d", n)
		}
	}

	// Shutdown
	cons2a.Shutdown()
	cons1b.Shutdown()
	cons2b.Shutdown()
}

// TestTaskResurrectionInt ensures that a Claim won't recreate a task that had
// been deleted (marked as done). taskmgr has a non-integration version of this
// test.
func TestTaskResurrectionInt(t *testing.T) {
	etcdv3c := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(etcdv3c)
	c := context.Background()
	t.Parallel()

	kvc.Delete(c, "/test-resurrect", etcdv3.WithPrefix())

	task := metcdv3.DefaultTaskFunc("xyz", "")

	conf := metcdv3.NewConfig("testclient", "/test-resurrect")
	coord := metcdv3.NewEtcdV3Coordinator(conf, etcdv3c)
	if err := coord.Init(nil); err != nil {
		t.Fatalf("Error initializing coordinator: %v", err)
	}
	defer coord.Close()

	// Try to claim a nonexistent
	if claimed := coord.Claim(task); claimed {
		t.Fatal("Claiming a nonexistent task should not work but did!")
	}

	// Create a task, mark it as done, and try to claim it again
	client := metcdv3.NewClient("/test-resurrect", etcdv3c)
	if err := client.SubmitTask(metcdv3.DefaultTaskFunc("xyz", "")); err != nil {
		t.Fatalf("Error submitting task xyz: %v", err)
	}

	if claimed := coord.Claim(task); !claimed {
		t.Fatal("Failed to claim task xyz")
	}

	coord.Done(task)

	if claimed := coord.Claim(task); claimed {
		t.Fatal("Reclaimed task that was marked as done.")
	}
}
