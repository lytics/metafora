package m_etcd

import (
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd/testutil"
)

/*
	Running the Integration Test:

ETCDTESTS=1 go test -v ./...
*/

func TestCoordinatorFirstNodeJoiner(t *testing.T) {
	coordinator1, _ := setupEtcd(t)
	defer coordinator1.Close()
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	client, _ := testutil.NewEtcdClient(t)

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
func TestCoordinatorTC1(t *testing.T) {
	coordinator1, _ := setupEtcd(t)
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()
	client, _ := testutil.NewEtcdClient(t)

	tasks := make(chan string)
	task001 := "test-task"
	taskPath := path.Join(namespace, TasksPath, task001)
	errc := make(chan error)

	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		errc <- coordinator1.Watch(tasks)
	}()

	client.CreateDir(taskPath, 5)

	select {
	case taskId := <-tasks:
		if taskId != task001 {
			t.Fatalf("coordinator1.Watch() test failed: We received the incorrect taskId.  Got [%s] Expected[%s]", taskId, task001)
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("coordinator1.Watch() test failed: The testcase timed out after 5 seconds.")
	}

	coordinator1.Close()
	err := <-errc
	if err != nil {
		t.Fatalf("coordinator1.Watch() returned an err: %v", err)
	}
}

// Submit a task while a coordinator is actively watching for tasks.
func TestCoordinatorTC2(t *testing.T) {
	coordinator1, client := setupEtcd(t)
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()

	testTasks := []string{"test1", "test2", "test3"}

	mclient := NewClient(namespace, client)

	tasks := make(chan string)
	errc := make(chan error)
	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		errc <- coordinator1.Watch(tasks)
	}()

	for _, task := range testTasks {
		err := mclient.SubmitTask(task)
		if err != nil {
			t.Fatalf("Error submitting a task to metafora via the client.  Error:\n%v", err)
		}
		recvd := <-tasks
		if recvd != task {
			t.Fatalf("%s != %s - received an unexpected task", recvd, task)
		}
		if ok := coordinator1.Claim(task); !ok {
			t.Fatal("coordinator1.Claim() unable to claim the task")
		}
	}

	coordinator1.Close()
	err := <-errc
	if err != nil {
		t.Fatalf("coordinator1.Watch() returned an err: %v", err)
	}
}

// Start two coordinators to ensure that fighting over claims results in only
// one coordinator winning (and the other not crashing).
func TestCoordinatorTC3(t *testing.T) {
	coordinator1, hosts := setupEtcd(t)
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()
	c2, _ := NewEtcdCoordinator("node2", namespace, hosts)
	coordinator2 := c2.(*EtcdCoordinator)
	if err := coordinator2.Init(newCtx(t, "coordinator2")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator2.Close()

	testTasks := []string{"test-claiming-task0001", "test-claiming-task0002", "test-claiming-task0003"}

	mclient := NewClient(namespace, hosts)

	// Start the watchers
	errc := make(chan error, 2)
	c1tasks := make(chan string)
	c2tasks := make(chan string)
	go func() {
		errc <- coordinator1.Watch(c1tasks)
	}()
	go func() {
		errc <- coordinator2.Watch(c2tasks)
	}()

	// Submit the tasks
	for _, tid := range testTasks {
		err := mclient.SubmitTask(tid)
		if err != nil {
			t.Fatalf("Error submitting task=%q to metafora via the client. Error:\n%v", tid, err)
		}
	}

	//XXX This assumes tasks are sent by watchers in the order they were
	//    submitted to etcd which, while /possible/ to guarantee, isn't a gurantee
	//    we're interested in making. Remove this section if it starts causing problems.
	//    We only want to guarantee that exactly one coordinator can claim a task.
	c1tid := <-c1tasks
	c2tid := <-c2tasks
	if c1tid != c2tid {
		t.Fatalf("Watchers didn't receive the same task %s != %s. Might be fine; see code.", c1tid, c2tid)
	}

	// Make sure c1 can claim and c2 cannot
	if ok := coordinator1.Claim(c1tid); !ok {
		t.Fatalf("coordinator1.Claim() unable to claim the task=%q", c1tid)
	}
	if ok := coordinator2.Claim(c1tid); ok {
		t.Fatalf("coordinator2.Claim() succeeded for task=%q when it shouldn't have!", c2tid)
	}

	// Make sure coordinators close down properly and quickly
	coordinator1.Close()
	if err := <-errc; err != nil {
		t.Errorf("Error shutting down coordinator1: %v", err)
	}
	coordinator2.Close()
	if err := <-errc; err != nil {
		t.Errorf("Error shutting down coordinator2: %v", err)
	}
}

// Submit a task before any coordinators are active.  Then start a coordinator to
// ensure the tasks are picked up by the new coordinator
//
// Then call coordinator.Release() on the task to make sure a coordinator picks it
// up again.
func TestCoordinatorTC4(t *testing.T) {
	coordinator1, hosts := setupEtcd(t)

	task := "testtask4"

	mclient := NewClient(namespace, hosts)

	err := mclient.SubmitTask(task)
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client. Error:\n%v", err)
	}

	// Don't start up the coordinator until after the metafora client has submitted work.
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()

	errc := make(chan error)
	c1tasks := make(chan string)
	go func() {
		errc <- coordinator1.Watch(c1tasks)
	}()

	tid := <-c1tasks

	if ok := coordinator1.Claim(tid); !ok {
		t.Fatal("coordinator1.Claim() unable to claim the task")
	}

	// Startup a second
	c2, _ := NewEtcdCoordinator("node2", namespace, hosts)
	coordinator2 := c2.(*EtcdCoordinator)
	if err := coordinator2.Init(newCtx(t, "coordinator2")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator2.Close()

	c2tasks := make(chan string)
	go func() {
		errc <- coordinator2.Watch(c2tasks)
	}()

	// coordinator 2 shouldn't see anything yet
	select {
	case <-c2tasks:
		t.Fatal("coordinator2.Watch() returned a task when there are none to claim!")
	case <-time.After(100 * time.Millisecond):
	}

	// Now release the task from coordinator1 and claim it with coordinator2
	coordinator1.Release(tid)
	tid = <-c2tasks
	if ok := coordinator2.Claim(tid); !ok {
		t.Fatalf("coordinator2.Claim() should have succeded on released task=%q", tid)
	}

	coordinator1.Close()
	coordinator2.Close()
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Errorf("coordinator returned an error after closing: %v", err)
		}
	}
}

// TestNodeCleanup ensures the coordinator properly cleans up its node entry
// upon exit.
func TestNodeCleanup(t *testing.T) {
	c1, hosts := setupEtcd(t)
	if err := c1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	c2, _ := NewEtcdCoordinator("node2", namespace, hosts)
	if err := c2.Init(newCtx(t, "coordinator2")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer c1.Close()
	defer c2.Close()

	// Make sure node directories were created
	client, _ := testutil.NewEtcdClient(t)
	resp, err := client.Get(namespace+"/nodes/"+nodeID, false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}

	resp, err = client.Get(namespace+"/nodes/node2", false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}

	// Shutdown one and make sure its node directory is gone
	c1.Close()

	resp, err = client.Get(namespace+"/nodes/"+nodeID, false, false)
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
	resp, err = client.Get(namespace+"/nodes/node2", false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}
}

// TestNodeRefresher ensures the node refresher properly updates the TTL on the
// node directory in etcd and shuts down the entire consumer on error.
func TestNodeRefresher(t *testing.T) {
	// make -race happy by using atomic to fiddle with ttl
	orig := atomic.LoadUint64(&DefaultNodePathTTL)
	atomic.StoreUint64(&DefaultNodePathTTL, 3)
	defer atomic.StoreUint64(&DefaultNodePathTTL, orig)

	coord, _ := setupEtcd(t)
	hf := metafora.HandlerFunc(nil) // we won't be handling any tasks
	consumer, err := metafora.NewConsumer(coord, hf, metafora.DumbBalancer)
	if err != nil {
		t.Fatalf("Error creating consumer: %+v", err)
	}
	client, _ := testutil.NewEtcdClient(t)

	defer consumer.Shutdown()
	runDone := make(chan struct{})
	go func() {
		consumer.Run()
		close(runDone)
	}()

	nodePath := path.Join(namespace, "nodes", coord.NodeID)
	ttl := int64(-1)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, _ := client.Get(nodePath, false, false)
		if resp != nil && resp.Node.Dir {
			ttl = resp.Node.TTL
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if ttl == -1 {
		t.Fatalf("Node path %s not found.", nodePath)
	}
	if ttl < 1 || ttl > 3 {
		t.Fatalf("Expected TTL to be between 1 and 3, found: %d", ttl)
	}

	// Let it refresh once to make sure that works
	time.Sleep(time.Duration(ttl) * time.Second)
	ttl = -1
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, _ := client.Get(nodePath, false, false)
		if resp != nil && resp.Node.Dir {
			ttl = resp.Node.TTL
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if ttl == -1 {
		t.Fatalf("Node path %s not found.", nodePath)
	}

	// Now remove the node out from underneath the refresher to cause it to fail
	if _, err := client.Delete(nodePath, true); err != nil {
		t.Fatalf("Unexpected error deleting %s: %+v", nodePath, err)
	}

	select {
	case <-runDone:
		// success! run exited
	case <-time.After(5 * time.Second):
		t.Fatal("Consumer didn't exit even though node directory disappeared!")
	}
}
