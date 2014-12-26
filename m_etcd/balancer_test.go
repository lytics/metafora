package m_etcd

import (
	"testing"
	"time"

	"github.com/lytics/metafora"
)

func TestFairBalancer(t *testing.T) {
	coord1, etcdc := setupEtcd(t)
	coord2 := NewEtcdCoordinator("node2", namespace, etcdc).(*EtcdCoordinator)

	cli := NewClient(namespace, etcdc)

	h := metafora.SimpleHandler(func(task string, stop <-chan bool) bool {
		t.Logf("Starting %s", task)
		<-stop
		t.Logf("Stopping %s", task)
		return false // never done
	})

	// Create two consumers
	b1 := NewFairBalancer(nodeID, namespace, etcdc)
	con1, err := metafora.NewConsumer(coord1, h, b1)
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewFairBalancer("node2", namespace, etcdc)
	con2, err := metafora.NewConsumer(coord2, h, b2)
	if err != nil {
		t.Fatal(err)
	}

	// Start the first and let it claim a bunch of tasks
	go con1.Run()
	defer con1.Shutdown()
	cli.SubmitTask("t1")
	cli.SubmitTask("t2")
	cli.SubmitTask("t3")
	cli.SubmitTask("t4")
	cli.SubmitTask("t5")
	cli.SubmitTask("t6")

	time.Sleep(500 * time.Millisecond)

	if len(con1.Tasks()) != 6 {
		t.Fatalf("con1 should have claimed 6 tasks: %d", len(con1.Tasks()))
	}

	// Start the second consumer and force the 1st to rebalance
	go con2.Run()
	defer con2.Shutdown()

	// Wait for node to startup and register
	time.Sleep(500 * time.Millisecond)

	cli.SubmitCommand(nodeID, metafora.CommandBalance())

	time.Sleep(2 * time.Second)

	c1Tasks := con1.Tasks()
	c2Tasks := con2.Tasks()
	if len(c1Tasks) != 4 || len(c2Tasks) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks), len(c2Tasks))
	}

	// Finally make sure that balancing the other node does nothing
	cli.SubmitCommand("node2", metafora.CommandBalance())

	time.Sleep(2 * time.Second)

	c1Tasks2 := con1.Tasks()
	c2Tasks2 := con2.Tasks()
	if len(c1Tasks2) != 4 || len(c2Tasks2) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks), len(c2Tasks))
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
