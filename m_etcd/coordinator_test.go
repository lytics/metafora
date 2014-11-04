package m_etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

/*
	Running the Integration Test:
	#if you don't have etcd install use this script to set it up:
	sudo bash ./scripts/docker_run_etcd.sh

ETCDTESTS=1 go test -v ./...
*/

func TestCoordinatorFirstNodeJoiner(t *testing.T) {
	coordinator1, client := setupEtcd(t)
	defer coordinator1.Close()
	coordinator1.Init(newCtx(t, "coordinator1"))

	if coordinator1.TaskPath != namespace+"/tasks" {
		t.Fatalf("TestFailed: TaskPath should be \"/%s/tasks\" but we got \"%s\"", namespace, coordinator1.TaskPath)
	}

	const sorted = false
	const recursive = false
	_, err := client.Get(namespace+"/tasks", sorted, recursive)
	if err != nil && strings.Contains(err.Error(), "Key not found") {
		t.Fatalf("The tasks path wasn't created when the first node joined: path[%s]", namespace+"/tasks")
	} else if err != nil {
		t.Fatalf("Unknown error trying to test: err: %s", err.Error())
	}

	//TODO test for node path too...

}

// Ensure that Watch() picks up new tasks and returns them.
//
func TestCoordinatorTC1(t *testing.T) {
	coordinator1, client := setupEtcd(t)
	defer coordinator1.Close()

	coordinator1.Init(newCtx(t, "coordinator1"))

	watchRes := make(chan string)
	task001 := "test-task0001"
	fullTask001Path := coordinator1.TaskPath + "/" + task001

	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator1.Watch()
		if err != nil {
			t.Fatalf("coordinator1.Watch() returned an err: %v", err)
		}
		t.Logf("We got a task id from the coordinator1.Watch() res:%s", taskId)

		watchRes <- taskId
	}()

	client.CreateDir(fullTask001Path, 5)

	select {
	case taskId := <-watchRes:
		if taskId != task001 {
			t.Fatalf("coordinator1.Watch() test failed: We received the incorrect taskId.  Got [%s] Expected[%s]", taskId, task001)
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("coordinator1.Watch() test failed: The testcase timed out after 5 seconds.")
	}
}

//   Submit a task while a coordinator is actively watching for tasks.
//
func TestCoordinatorTC2(t *testing.T) {
	coordinator1, client := setupEtcd(t)
	coordinator1.Init(newCtx(t, "coordinator1"))
	defer coordinator1.Close()

	test_finished := make(chan bool)
	testTasks := []string{"test-claiming-task0001", "test-claiming-task0002", "test-claiming-task0003"}

	mclient := NewClientWithLogger(namespace, client, testLogger{"metafora-client1", t})

	startATaskWatcher := func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator1.Watch()
		if err != nil {
			t.Fatalf("coordinator1.Watch() returned an err: %v", err)
		}

		t.Logf("We got a task id from the coordinator1.Watch() res: %s", taskId)

		if ok := coordinator1.Claim(taskId); !ok {
			t.Fatal("coordinator1.Claim() unable to claim the task")
		}

		test_finished <- true
	}

	go startATaskWatcher()
	time.Sleep(24 * time.Millisecond)
	err := mclient.SubmitTask(testTasks[0])
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client.  Error:\n%v", err)
	}

	select {
	case res := <-test_finished:
		if !res {
			t.Fatalf("Background test checker failed so the test failed.")
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("Test failed: The testcase timed out after 5 seconds.")
	}
}

//   1) Submit two tasks between calls to coordinator.Watch() to make sure the
//   coordinator picks up tasks made between requests to Watch().
//
//   2) Try claiming the same taskId twice.
//
func TestCoordinatorTC3(t *testing.T) {
	coordinator1, client := setupEtcd(t)
	coordinator1.Init(newCtx(t, "coordinator1"))
	defer coordinator1.Close()
	coordinator2 := NewEtcdCoordinator("node2", namespace, client).(*EtcdCoordinator)
	coordinator2.Init(newCtx(t, "coordinator2"))
	defer coordinator2.Close()

	test_finished := make(chan bool)
	testTasks := []string{"test-claiming-task0001", "test-claiming-task0002", "test-claiming-task0003"}

	mclient := NewClientWithLogger(namespace, client, testLogger{"metafora-client1", t})

	startATaskWatcher := func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator1.Watch()
		if err != nil {
			t.Fatalf("coordinator1.Watch() returned an err: %v", err)
		}

		_, err = coordinator2.Watch() //coordinator2 should also pickup this task
		if err != nil {
			t.Fatalf("coordinator2.Watch() returned an err: %v", err)
		}

		t.Logf("We got a task id from the coordinator1.Watch() res: %s", taskId)

		if ok := coordinator1.Claim(taskId); !ok {
			t.Fatal("coordinator1.Claim() unable to claim the task")
		}

		//Try to claim the task in a second coordinator.  Should fail
		if ok := coordinator2.Claim(taskId); ok {
			t.Fatal("coordinator1.Claim() unable to claim the task")
		}

		test_finished <- true
	}

	err := mclient.SubmitTask(testTasks[1])
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client. Error:\n%v", err)
	}
	err = mclient.SubmitTask(testTasks[2])
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client. Error:\n%v", err)
	}

	go startATaskWatcher()
	select {
	case res := <-test_finished:
		if !res {
			t.Fatalf("Background test checker failed so the test failed.")
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("Test failed: The testcase timed out after 5 seconds.")
	}
	go startATaskWatcher()
	select {
	case res := <-test_finished:
		if !res {
			t.Fatalf("Background test checker failed so the test failed.")
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("Test failed: The testcase timed out after 5 seconds.")
	}
}

// Submit a task before any coordinators are active.  Then start a coordinator to
// ensure the tasks are picked up by the new coordinator
//
// Then call coordinator.Release() on the task to make sure a coordinator picks it
// up again.
func TestCoordinatorTC4(t *testing.T) {
	coordinator1, client := setupEtcd(t)

	watchOk := make(chan bool)
	task := "testtask4"

	mclient := NewClientWithLogger(namespace, client, testLogger{"metafora-client1", t})

	err := mclient.SubmitTask(task)
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client. Error:\n%v", err)
	}

	// Don't start up the coordinator until after the metafora client has submitted work.
	coordinator1.Init(newCtx(t, "coordinator1"))
	defer coordinator1.Close()

	coord1Watch := func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator1.Watch()
		if err != nil {
			t.Fatalf("coordinator1.Watch() returned an err: %v", err)
		}

		t.Logf("Task from the coordinator1.Watch(): %s", taskId)

		if ok := coordinator1.Claim(taskId); !ok {
			t.Fatal("coordinator1.Claim() unable to claim the task")
		}

		watchOk <- true
	}

	go coord1Watch()
	select {
	case <-watchOk:
	case <-time.After(time.Second * 5):
		t.Fatalf("Test failed: The testcase timed out after 5 seconds.")
	}

	//Testcase2 test releasing a task,  Since coordinator1 is still running
	// it should be able to pick up the task again.
	t.Logf("Releasing %s", task)
	coordinator1.Release(task)

	t.Log("Watching again")
	go coord1Watch()
	select {
	case <-watchOk:
	case <-time.After(time.Second * 5):
		t.Fatalf("Test failed: The testcase timed out after 5 seconds.")
	}
}

// Test that Watch() picks up new tasks and returns them.
// Then Claim() the task and make sure we are able to claim it.
//     Calling Claim() should also trigger a scheduled refresh of the claim before it's ttl
// Test after (ClaimTTL + 1 second) that the claim is still around.
// Then add a second coordinator and kill the first one.  The second coordinator
// should pick up the work from the dead first one.
func TestClaimRefreshExpire(t *testing.T) {
	coordinator1, client := setupEtcd(t)
	coordinator1.ClaimTTL = 1
	defer coordinator1.Close()
	coordinator1.Init(newCtx(t, "coordinator1"))
	coord1ResultChannel := make(chan string)

	mclient := NewClientWithLogger(namespace, client, testLogger{"metafora-client1", t})
	task001 := "test-task0001"

	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator1.Watch()
		if err != nil {
			t.Fatalf("coordinator1.Watch() returned an err: %v", err)
		}
		t.Logf("We got a task id from the coordinator1.Watch() res:%s", taskId)

		coordinator1.Claim(taskId)
		coord1ResultChannel <- taskId
	}()

	err := mclient.SubmitTask(task001)
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client. Error:\n%v", err)
	}

	//Step 1 : Make sure we picked up and claimed the task before moving on...
	select {
	case taskId := <-coord1ResultChannel:
		if taskId != task001 {
			t.Fatalf("coordinator1.Watch() test failed: We received the incorrect taskId.  Got [%s] Expected[%s]", taskId, task001)
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("coordinator1.Watch() test failed: The testcase timed out after 5 seconds.")
	}

	//start a second coordinator and make sure it can't claim our task.
	coordinator2 := NewEtcdCoordinator("node2", namespace, client).(*EtcdCoordinator)
	coordinator2.ClaimTTL = 1
	defer coordinator2.Close()
	coordinator2.Init(newCtx(t, "coordinator2"))
	coord2ResultChannel := make(chan string)
	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator2.Watch()
		if err != nil {
			t.Fatalf("coordinator2.Watch() returned an err: %v", err)
		}
		t.Logf("We got a task id from the coordinator2.Watch() res:%s", taskId)
		coordinator2.Claim(taskId)
		coord2ResultChannel <- taskId
	}()

	//make sure we still have the claim after 2 seconds
	select {
	case taskId := <-coord2ResultChannel:
		t.Fatalf("coordinator2.Watch() test failed: We received a taskId when we shouldn't have.  Got [%s]", taskId)
	case <-time.After(3 * time.Second):
	}

	// This should shut down coordinator1's task watcher and refresher, so that all its tasks are returned
	// and coordinator2 should pick them up.
	t.Log("Coordinator1 trying to shutdown coordinator1. ")
	go func() {
		// The only way to tell when coord.Close() finishes is by waiting for a Watch()
		// to exit.
		coordinator1.Watch()
		coord1ResultChannel <- ""
	}()
	coordinator1.Close()
	<-coord1ResultChannel
	t.Log("Coordinator1 was closed, so its tasks should shortly become available again. ")

	//Now that coordinator1 is shutdown coordinator2 should reover it's tasks.
	select {
	case taskId := <-coord2ResultChannel:
		if taskId != task001 {
			t.Fatalf("coordinator2.Watch() test failed: We received the incorrect taskId.  Got [%s] Expected[%s]", taskId, task001)
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("coordinator2.Watch() test failed: The testcase timed out before coordinator2 recovered coordinator1's tasks.")
	}
}

// TestNodeCleanup ensures the coordinator properly cleans up its node entry
// upon exit.
func TestNodeCleanup(t *testing.T) {
	c1, client := setupEtcd(t)
	c1.Init(newCtx(t, "coordinator1"))
	c2 := NewEtcdCoordinator("node2", namespace, client).(*EtcdCoordinator)
	c2.Init(newCtx(t, "coordinator2"))
	defer c1.Close()
	defer c2.Close()

	// Make sure node directories were created
	resp, err := client.Get(namespace+"/nodes/"+c1.NodeID, false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}

	resp, err = client.Get(namespace+"/nodes/"+c2.NodeID, false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}

	// Shutdown one and make sure its node directory is gone
	c1.Close()

	resp, err = client.Get(namespace+"/nodes/"+c1.NodeID, false, false)
	if err != nil {
		if eerr, ok := err.(*etcd.EtcdError); !ok {
			t.Errorf("Unexpected error %T retrieving node key from etcd: %v", err, err)
		} else {
			if eerr.ErrorCode != EcodeKeyNotFound {
				t.Errorf("Expected error code %d but found %v", eerr.ErrorCode, err)
			}
			// error code was ok! (100)
		}
	} else {
		t.Errorf("Expected Not Found error, but directory still exists!")
	}

	// Make sure c2 is untouched
	resp, err = client.Get(namespace+"/nodes/"+c2.NodeID, false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}
}
