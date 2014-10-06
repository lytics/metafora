package m_etcd

// NOTES
//
// These tests are in reality integration tests which require that
// etcd is running on the test system and its peers are found
// in the ENV variable ETCDCTL_PEERS. The tests do not clean
// out data and require a fresh set of etcd instances for
// each run. You can consider this a known bug which
// will be fixed in a future release.
//
// See: https://github.com/lytics/metafora/issues/31

import (
	"github.com/coreos/go-etcd/etcd"
	"os"
	"strings"
	"testing"
)

const (
	NAMESPACE  = `test`
	NODES_DIR  = `/test/nodes`
	NODE1      = `node1`
	NODE1_PATH = NODES_DIR + `/` + NODE1
	COMMAND    = `{"command":"testing"}`
)

// TestNodes tests that client.Nodes() returns the metafora nodes
// registered in etcd.
func TestNodes(t *testing.T) {
	skipEtcd(t)

	eclient := newEtcdClient(t)

	mclient := NewClient(NAMESPACE, eclient)

	if _, err := eclient.CreateDir(NODE1_PATH, 0); err != nil {
		t.Fatalf("AddChild %v returned error: %v", NODES_DIR, err)
	}

	if nodes, err := mclient.Nodes(); err != nil {
		t.Fatalf("Nodes returned error: %v", err)
	} else {
		for i, n := range nodes {
			t.Logf("%v -> %v", i, n)
		}
	}
}

// TestSubmitTask tests that client.SubmitTask(...) adds a task to
// the proper path in etcd, and that the same task id cannot be
// submitted more than once.
func TestSubmitTask(t *testing.T) {
	skipEtcd(t)

	eclient := newEtcdClient(t)

	mclient := NewClient(NAMESPACE, eclient)

	if err := mclient.SubmitTask("testid1"); err != nil {
		t.Fatalf("Submit task failed on initial submission, error: %v", err)
	}

	if err := mclient.SubmitTask("testid1"); err == nil {
		t.Fatalf("Submit task did not fail, but should of, when using existing tast id")
	}
}

// TestSubmitCommand tests that client.SubmitCommand(...) adds a command
// to the proper node path in etcd, and that it can be read back.
func TestSubmitCommand(t *testing.T) {
	skipEtcd(t)

	eclient := newEtcdClient(t)

	mclient := NewClient(NAMESPACE, eclient)

	if err := mclient.SubmitCommand(NODE1, COMMAND); err != nil {
		t.Fatalf("Unable to submit command.   error:%v", err)
	}

	if res, err := eclient.Get(NODES_DIR, false, false); err != nil {
		t.Fatalf("Get on path %v returned error: %v", NODES_DIR, err)
	} else if res.Node == nil || res.Node.Nodes == nil {
		t.Fatalf("Get on path %v returned nil for child nodes", NODES_DIR)
	} else {
		for i, n := range res.Node.Nodes {
			t.Logf("%v -> %v", i, n)
		}
	}
}

// newEtcdClient creates a new etcd client for use by the metafora client during testing.
func newEtcdClient(t *testing.T) *etcd.Client {
	// This is the same ENV variable that etcdctl uses for peers.
	peers_from_environment := os.Getenv("ETCDCTL_PEERS")

	if peers_from_environment == "" {
		peers_from_environment = "localhost:5001,localhost:5002,localhost:5003"
	}

	peers := strings.Split(peers_from_environment, ",")

	eclient := etcd.NewClient(peers)

	if ok := eclient.SyncCluster(); !ok {
		t.Fatalf("Cannot sync etcd cluster using peers: %v", strings.Join(peers, ", "))
	}

	if !isEtcdUp(eclient, t) {
		t.Fatalf("Cannot connect to etcd using peers: %v", strings.Join(peers, ", "))
	}

	return eclient
}
