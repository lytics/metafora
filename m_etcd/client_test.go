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
	"path"
	"testing"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
	"golang.org/x/net/context"
)

// TestNodes tests that client.Nodes() returns the metafora nodes
// registered in etcd.
func TestNodes(t *testing.T) {
	ctx := setupEtcd(t)
	//defer ctx.Cleanup()

	key := path.Join(ctx.Conf.Namespace, NodesPath, ctx.Conf.Name)
	opts := &client.SetOptions{PrevExist: client.PrevNoExist, Dir: true}
	if _, err := ctx.EtcdClient.Set(context.TODO(), key, "", opts); err != nil {
		t.Fatalf("Error creating node key %q: %v", key, err)
	}

	if nodes, err := ctx.MClient.Nodes(); err != nil {
		t.Fatalf("Nodes returned error: %v", err)
	} else {
		for i, n := range nodes {
			t.Logf("%v -> %v", i, n)
		}
		if len(nodes) != 1 {
			t.Fatalf("Expected 1 node but found %d", len(nodes))
		}
		if nodes[0] != ctx.Conf.Name {
			t.Fatalf("Expected node %q but found %q", ctx.Conf.Name, nodes[0])
		}
	}
}

// TestSubmitTask tests that client.SubmitTask(...) adds a task to
// the proper path in etcd, and that the same task id cannot be
// submitted more than once.
func TestSubmitTask(t *testing.T) {
	ctx := setupEtcd(t)
	defer ctx.Cleanup()

	task := DefaultTaskFunc("testid1", "")

	if err := ctx.MClient.SubmitTask(task); err != nil {
		t.Fatalf("Submit task failed on initial submission, error: %v", err)
	}

	if err := ctx.MClient.SubmitTask(task); err == nil {
		t.Fatalf("Submit task did not fail, but should of, when using existing tast id")
	}
}

// TestSubmitCommand tests that client.SubmitCommand(...) adds a command
// to the proper node path in etcd, and that it can be read back.
func TestSubmitCommand(t *testing.T) {
	ctx := setupEtcd(t)
	defer ctx.Cleanup()

	if err := ctx.MClient.SubmitCommand(ctx.Conf.Name, metafora.CommandFreeze()); err != nil {
		t.Fatalf("Unable to submit command.   error:%v", err)
	}

	nodepath := path.Join(ctx.Conf.Namespace, NodesPath)
	if res, err := ctx.EtcdClient.Get(context.TODO(), nodepath, &client.GetOptions{Recursive: true}); err != nil {
		t.Fatalf("Get on path %v returned error: %v", nodepath, err)
	} else if res.Node == nil || len(res.Node.Nodes) == 0 {
		t.Fatalf("Get on path %v returned nil for child nodes", nodepath)
	}
}
