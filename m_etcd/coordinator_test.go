package m_etcd

import (
	"path"
	"strings"
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

	tasks := make(chan metafora.Task)
	task001 := &task{id: "test-task"}
	taskPath := path.Join(namespace, TasksPath, task001.ID())
	errc := make(chan error)

	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		errc <- coordinator1.Watch(tasks)
	}()

	client.CreateDir(taskPath, 5)

	select {
	case task := <-tasks:
		if task.ID() != task001.ID() {
			t.Fatalf("coordinator1.Watch() test failed: We received the incorrect taskId.  Got [%s] Expected[%s]", task, task001)
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
	coordinator1, conf := setupEtcd(t)
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()

	testTasks := []string{"test1", "test2", "test3"}

	mclient := NewClient(conf.Namespace, conf.Hosts)

	tasks := make(chan metafora.Task)
	errc := make(chan error)
	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		errc <- coordinator1.Watch(tasks)
	}()

	for _, taskid := range testTasks {
		err := mclient.SubmitTask(DefaultTaskFunc(taskid, ""))
		if err != nil {
			t.Fatalf("Error submitting a task to metafora via the client.  Error:\n%v", err)
		}
		recvd := <-tasks
		if recvd.ID() != taskid {
			t.Fatalf("%s != %s - received an unexpected task", recvd.ID(), taskid)
		}
		if ok := coordinator1.Claim(recvd); !ok {
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
	coordinator1, conf1 := setupEtcd(t)
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()
	conf2 := conf1.Copy()
	conf2.Name = "node2"
	coordinator2, _ := NewEtcdCoordinator(conf2)
	if err := coordinator2.Init(newCtx(t, "coordinator2")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator2.Close()

	testTasks := []string{"test-claiming-task0001", "test-claiming-task0002", "test-claiming-task0003"}

	mclient := NewClient(conf1.Namespace, conf1.Hosts)

	// Start the watchers
	errc := make(chan error, 2)
	c1tasks := make(chan metafora.Task)
	c2tasks := make(chan metafora.Task)
	go func() {
		errc <- coordinator1.Watch(c1tasks)
	}()
	go func() {
		errc <- coordinator2.Watch(c2tasks)
	}()

	// Submit the tasks
	for _, tid := range testTasks {
		err := mclient.SubmitTask(DefaultTaskFunc(tid, ""))
		if err != nil {
			t.Fatalf("Error submitting task=%q to metafora via the client. Error:\n%v", tid, err)
		}
	}

	//XXX This assumes tasks are sent by watchers in the order they were
	//    submitted to etcd which, while /possible/ to guarantee, isn't a gurantee
	//    we're interested in making.
	//    We only want to guarantee that exactly one coordinator can claim a task.
	c1t := <-c1tasks
	c2t := <-c2tasks
	if c1t.ID() != c2t.ID() {
		t.Logf("Watchers didn't receive the same task %s != %s. It's fine; watch order isn't guaranteed", c1t, c2t)
	}

	// Make sure c1 can claim and c2 cannot
	if ok := coordinator1.Claim(c1t); !ok {
		t.Fatalf("coordinator1.Claim() unable to claim the task=%q", c1t)
	}
	if ok := coordinator2.Claim(c1t); ok {
		t.Fatalf("coordinator2.Claim() succeeded for task=%q when it shouldn't have!", c2t)
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
	coordinator1, conf1 := setupEtcd(t)

	task := "testtask4"

	mclient := NewClient(conf1.Namespace, conf1.Hosts)

	err := mclient.SubmitTask(DefaultTaskFunc(task, ""))
	if err != nil {
		t.Fatalf("Error submitting a task to metafora via the client. Error:\n%v", err)
	}

	// Don't start up the coordinator until after the metafora client has submitted work.
	if err := coordinator1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator1.Close()

	errc := make(chan error)
	c1tasks := make(chan metafora.Task)
	go func() {
		errc <- coordinator1.Watch(c1tasks)
	}()

	tid := <-c1tasks

	if ok := coordinator1.Claim(tid); !ok {
		t.Fatal("coordinator1.Claim() unable to claim the task")
	}

	// Startup a second
	conf2 := conf1.Copy()
	conf2.Name = "node2"
	coordinator2, _ := NewEtcdCoordinator(conf2)
	if err := coordinator2.Init(newCtx(t, "coordinator2")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer coordinator2.Close()

	c2tasks := make(chan metafora.Task)
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
	c1, conf1 := setupEtcd(t)
	if err := c1.Init(newCtx(t, "coordinator1")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	conf2 := conf1.Copy()
	conf2.Name = "node2"
	c2, _ := NewEtcdCoordinator(conf2)
	if err := c2.Init(newCtx(t, "coordinator2")); err != nil {
		t.Fatalf("Unexpected error initialzing coordinator: %v", err)
	}
	defer c1.Close()
	defer c2.Close()

	// Make sure node directories were created
	client, _ := testutil.NewEtcdClient(t)
	c1nodep := path.Join(conf1.Namespace, NodesPath, conf1.Name)
	resp, err := client.Get(c1nodep, false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}

	c2nodep := path.Join(conf2.Namespace, NodesPath, conf2.Name)
	resp, err = client.Get(c2nodep, false, false)
	if err != nil {
		t.Fatalf("Error retrieving node key from etcd: %v", err)
	}
	if !resp.Node.Dir {
		t.Error(resp.Node.Key + " isn't a directory!")
	}

	// Shutdown one and make sure its node directory is gone
	c1.Close()

	resp, err = client.Get(c1nodep, false, false)
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
	resp, err = client.Get(c2nodep, false, false)
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
	t.Parallel()
	_, conf := setupEtcd(t)

	// Use a custom node path ttl
	conf.NodeTTL = 3
	coord, err := NewEtcdCoordinator(conf)
	if err != nil {
		t.Fatalf("Error creating coordinator: %v", err)
	}

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

	nodePath := path.Join(conf.Namespace, NodesPath, conf.Name)
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

// TestExpiration ensures that expired claims get reclaimed properly.
func TestExpiration(t *testing.T) {
	coord, _ := setupEtcd(t)
	claims := make(chan int, 10)
	hf := metafora.HandlerFunc(metafora.SimpleHandler(func(_ metafora.Task, stop <-chan bool) bool {
		claims <- 1
		<-stop
		return true
	}))
	consumer, err := metafora.NewConsumer(coord, hf, metafora.DumbBalancer)
	if err != nil {
		t.Fatalf("Error creating consumer: %+v", err)
	}
	client, _ := testutil.NewEtcdClient(t)

	_, err = client.Create(path.Join(namespace, TasksPath, "abc", OwnerMarker), `{"node":"--"}`, 1)
	if err != nil {
		t.Fatalf("Error creating fake claim: %v", err)
	}

	defer consumer.Shutdown()
	go consumer.Run()

	// Wait for claim to expire and coordinator to pick up task
	select {
	case <-claims:
		// Task claimed!
	case <-time.After(5 * time.Second):
		t.Fatal("Task not claimed long after it should have been.")
	}

	tasks := consumer.Tasks()
	if len(tasks) != 1 {
		t.Fatalf("Expected 1 task to be claimed but found: %v", tasks)
	}
}
