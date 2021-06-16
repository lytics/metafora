package metcdv3

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
	"context"
	"testing"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/metcdv3/testutil"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

const (
	Namespace = "test"
	NodesDir  = "/test/nodes"
	Node1     = "node1"
	Node1Path = "/test/nodes/node1"
)

// TestNodes tests that client.Nodes() returns the metafora nodes
// registered in etcd.
func TestNodes(t *testing.T) {
	c := context.Background()
	eclient := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(eclient)
	eclient.Delete(c, Node1Path, etcdv3.WithPrefix())

	mclient := NewClient(Namespace, eclient)

	if _, err := kvc.Put(c, Node1Path, "0"); err != nil {
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
	client := testutil.NewEtcdV3Client(t)
	mclient := NewClient(Namespace, client)

	task := DefaultTaskFunc("testid1", "")

	if err := mclient.DeleteTask(task.ID()); err != nil {
		t.Logf("DeleteTask returned an error, which maybe ok.  Error:%v", err)
	}

	if err := mclient.SubmitTask(task); err != nil {
		t.Fatalf("Submit task failed on initial submission, error: %v", err)
	}

	if err := mclient.SubmitTask(task); err == nil {
		t.Fatalf("Submit task did not fail, but should of, when using existing tast id")
	}
}

// TestSubmitCommand tests that client.SubmitCommand(...) adds a command
// to the proper node path in etcd, and that it can be read back.
func TestSubmitCommand(t *testing.T) {
	eclient := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(eclient)
	mclient := NewClient(Namespace, eclient)

	if err := mclient.SubmitCommand(Node1, metafora.CommandFreeze()); err != nil {
		t.Fatalf("Unable to submit command.   error:%v", err)
	}

	if res, err := kvc.Get(context.Background(), NodesDir, etcdv3.WithPrefix()); err != nil {
		t.Fatalf("Get on path %v returned error: %v", NodesDir, err)
	} else if res.Count == 0 {
		t.Fatalf("Get on path %v returned nil for child nodes", NodesDir)
	}
}
