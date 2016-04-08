package m_etcd

import (
	"testing"
	"time"

	"github.com/lytics/metafora"
)

func TestFairBalancer(t *testing.T) {
	t.Parallel()
	ctx := setupEtcd(t)
	defer ctx.Cleanup()
	coord1 := ctx.Coord
	conf1 := ctx.Conf
	conf2 := conf1.Copy()
	conf2.Name = "coord2"
	coord2, _ := NewEtcdCoordinator(conf2)

	h := metafora.SimpleHandler(func(task metafora.Task, stop <-chan bool) bool {
		metafora.Debugf("Starting %s", task.ID())
		<-stop
		metafora.Debugf("Stopping %s", task.ID())
		return false // never done
	})

	// Create two consumers
	b1 := NewFairBalancer(conf1, ctx.EtcdClient)
	con1, err := metafora.NewConsumer(coord1, h, b1)
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewFairBalancer(conf2, ctx.EtcdClient)
	con2, err := metafora.NewConsumer(coord2, h, b2)
	if err != nil {
		t.Fatal(err)
	}

	// Start the first and let it claim a bunch of tasks
	go con1.Run()
	defer con1.Shutdown()
	ctx.MClient.SubmitTask(DefaultTaskFunc("t1", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t2", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t3", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t4", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t5", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t6", ""))

	if got := waitForTasks(t, con1, 6); got != 6 {
		t.Fatalf("expected 6 but got %d", got)
	}

	// Start the second consumer and force the 1st to rebalance
	go con2.Run()
	defer con2.Shutdown()

	// Wait for node to startup and register
	waitForNodes(ctx.MClient, 2)

	if err := ctx.MClient.SubmitCommand(conf1.Name, metafora.CommandBalance()); err != nil {
		t.Fatalf("error sending balance command: %v", err)
	}

	// Wait for balance to finish
	assertBalanced := func() bool {
		c1Tasks := con1.Tasks()
		c2Tasks := con2.Tasks()
		return len(c1Tasks) == 4 && len(c2Tasks) == 2
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if assertBalanced() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	c1Tasks := con1.Tasks()
	c2Tasks := con2.Tasks()
	if !assertBalanced() {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks), len(c2Tasks))
	}

	// Finally make sure that balancing the other node does nothing
	ctx.MClient.SubmitCommand("node2", metafora.CommandBalance())

	time.Sleep(2 * time.Second)

	c1Tasks2 := con1.Tasks()
	c2Tasks2 := con2.Tasks()
	if len(c1Tasks2) != 4 || len(c2Tasks2) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks2), len(c2Tasks2))
	}
	for i := 0; i < 4; i++ {
		if c1Tasks[i] != c1Tasks2[i] {
			t.Errorf("task mismatch: %s != %s", c1Tasks[i], c1Tasks2[i])
		}
	}
	for i := 0; i < 2; i++ {
		if c2Tasks[i] != c2Tasks2[i] {
			t.Errorf("task mismatch: %s != %s", c2Tasks[i], c2Tasks2[i])
		}
	}
}

// Fair balancer shouldn't consider a shutting-down node
// See https://github.com/lytics/metafora/issues/92
func TestFairBalancerShutdown(t *testing.T) {
	ctx := setupEtcd(t)
	defer ctx.Cleanup()
	coord1 := ctx.Coord
	conf1 := ctx.Conf
	conf2 := conf1.Copy()
	conf2.Name = "node2"
	coord2, _ := NewEtcdCoordinator(conf2)

	// This handler always returns immediately
	h1 := metafora.SimpleHandler(func(task metafora.Task, stop <-chan bool) bool {
		metafora.Debugf("H1 Starting %s", task.ID())
		<-stop
		metafora.Debugf("H1 Stopping %s", task.ID())
		return false // never done
	})

	// Block forever on a single task
	stop2 := make(chan struct{})
	stopr := make(chan chan struct{}, 1)
	stopr <- stop2
	h2 := metafora.SimpleHandler(func(task metafora.Task, stop <-chan bool) bool {
		metafora.Debugf("H2 Starting %s", task.ID())
		blockchan, ok := <-stopr
		if ok {
			<-blockchan
		}
		<-stop
		metafora.Debugf("H2 Stopping %s", task.ID())
		return false // never done
	})

	// Create two consumers
	b1 := NewFairBalancer(conf1, ctx.EtcdClient)
	con1, err := metafora.NewConsumer(coord1, h1, b1)
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewFairBalancer(conf2, ctx.EtcdClient)
	con2, err := metafora.NewConsumer(coord2, h2, b2)
	if err != nil {
		t.Fatal(err)
	}

	// Start the first and let it claim a bunch of tasks
	go con1.Run()
	defer con1.Shutdown()
	ctx.MClient.SubmitTask(DefaultTaskFunc("t1", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t2", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t3", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t4", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t5", ""))
	ctx.MClient.SubmitTask(DefaultTaskFunc("t6", ""))

	if got := waitForTasks(t, con1, 6); got != 6 {
		t.Fatalf("expected 6 but got %d", got)
	}

	// Start the second consumer and force the 1st to rebalance
	go con2.Run()

	close(stopr)

	if err := ctx.MClient.SubmitCommand(conf1.Name, metafora.CommandBalance()); err != nil {
		t.Fatalf("Error submitting balance command to %q", conf1.Name)
	}

	if got := waitForTasks(t, con1, 4); got != 4 {
		t.Fatalf("expected 4 but got %d", got)
	}
	if got := waitForTasks(t, con2, 2); got != 2 {
		t.Fatalf("expected 2 but got %d", got)
	}

	// Make sure that balancing the other node does nothing
	ctx.MClient.SubmitCommand("node2", metafora.CommandBalance())

	time.Sleep(2 * time.Second)

	if got := waitForTasks(t, con1, 4); got != 4 {
		t.Fatalf("expected 4 but got %d", got)
	}
	if got := waitForTasks(t, con2, 2); got != 2 {
		t.Fatalf("expected 2 but got %d", got)
	}

	// Second consumer should block on a single task forever
	// Rebalancing the first node should then cause it to pickup all but
	// one task
	c2stop := make(chan struct{})
	go func() {
		con2.Shutdown()
		close(c2stop)
	}()

	if !waitForNodes(ctx.MClient, 1) {
		t.Fatalf("second consumer didn't shutdown")
	}

	ctx.MClient.SubmitCommand(conf1.Name, metafora.CommandBalance())

	if got := waitForTasks(t, con1, 5); got != 5 {
		t.Fatalf("expected 5 but got %d", got)
	}
	if got := waitForTasks(t, con2, 1); got != 1 {
		t.Fatalf("expected 1 but got %d", got)
	}

	// Now stop blocking task, rebalance and make sure the first node picked up the remaining
	close(stop2)

	time.Sleep(500 * time.Millisecond)

	// Consumer 2 should stop now
	<-c2stop

	ctx.MClient.SubmitCommand(conf1.Name, metafora.CommandBalance())

	// con2 is out of the picture. con1 has all the tasks.
	if got := waitForTasks(t, con1, 6); got != 6 {
		t.Fatalf("expected 6 but got %d", got)
	}
	if got := waitForTasks(t, con2, 0); got != 0 {
		t.Fatalf("expected 0 but got %d", got)
	}
}

// waitForNodes to startup and register
func waitForNodes(c metafora.Client, expected int) bool {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n, _ := c.Nodes()
		if len(n) == expected {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// waitForTasks to show up on a consumer
func waitForTasks(t *testing.T, c *metafora.Consumer, expected int) int {
	last := 0
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		last = len(c.Tasks())
		if last == expected {
			return last
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last
}
