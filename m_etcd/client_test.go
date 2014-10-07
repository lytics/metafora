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
	"os"
	"strings"
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

const (
	Namespace   = `test`
	NodesDir    = `/test/nodes`
	Node1       = `node1`
	Node1Path   = NodesDir + `/` + Node1
	CommandJson = `{"command":"testing"}`
)

// TestNodes tests that client.Nodes() returns the metafora nodes
// registered in etcd.
func TestNodes(t *testing.T) {
	eclient := newEtcdClient(t)

	mclient := NewClient(Namespace, eclient)

	if _, err := eclient.CreateDir(Node1Path, 0); err != nil {
		t.Fatalf("AddChild %v returned error: %v", NodesDir, err)
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
	eclient := newEtcdClient(t)

	mclient := NewClient(Namespace, eclient)

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
	eclient := newEtcdClient(t)

	mclient := NewClient(Namespace, eclient)

	if err := mclient.SubmitCommand(Node1, CommandJson); err != nil {
		t.Fatalf("Unable to submit command.   error:%v", err)
	}

	if res, err := eclient.Get(NodesDir, false, false); err != nil {
		t.Fatalf("Get on path %v returned error: %v", NodesDir, err)
	} else if res.Node == nil || res.Node.Nodes == nil {
		t.Fatalf("Get on path %v returned nil for child nodes", NodesDir)
	} else {
		for i, n := range res.Node.Nodes {
			t.Logf("%v -> %v", i, n)
		}
	}
}

// newEtcdClient creates a new etcd client for use by the metafora client during testing.
func newEtcdClient(t *testing.T) *etcd.Client {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}

	// This is the same ENV variable that etcdctl uses for peers.
	peerAddrs := os.Getenv("ETCDCTL_PEERS")

	if peerAddrs == "" {
		peerAddrs = "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003"
	}

	peers := strings.Split(peerAddrs, ",")

	eclient := etcd.NewClient(peers)

	if ok := eclient.SyncCluster(); !ok {
		t.Fatalf("Cannot sync etcd cluster using peers: %v", strings.Join(peers, ", "))
	}

	return eclient
}
