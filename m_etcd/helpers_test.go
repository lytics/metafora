package m_etcd

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

const (
	namespace = "/metaforatests"
	nodeID    = "node1"
)

// newEtcdClient creates a new etcd client for use by the metafora client during testing.
func newEtcdClient(t *testing.T) *etcd.Client {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}

	// This is the same ENV variable that etcdctl uses for peers.
	peerAddrs := os.Getenv("ETCD_PEERS")

	if peerAddrs == "" {
		peerAddrs = "127.0.0.1:4001"
	}

	peers := strings.Split(peerAddrs, ",")

	eclient := etcd.NewClient(peers)

	if ok := eclient.SyncCluster(); !ok {
		t.Fatalf("Cannot sync etcd cluster using peers: %v", strings.Join(peers, ", "))
	}

	eclient.SetConsistency(etcd.STRONG_CONSISTENCY)

	return eclient
}

// setupEtcd should be used for all etcd integration tests. It handles the following tasks:
//  * Skip tests if ETCDTESTS is unset
//  * Create and return an etcd client
//  * Create and return an initial etcd coordinator
func setupEtcd(t *testing.T) (*EtcdCoordinator, *etcd.Client) {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}
	client := newEtcdClient(t)
	const recursive = true
	client.Delete(namespace, recursive)
	return NewEtcdCoordinator(nodeID, namespace, client).(*EtcdCoordinator), client
}

type testLogger struct {
	prefix string
	*testing.T
}

func (l testLogger) Log(lvl metafora.LogLevel, m string, v ...interface{}) {
	l.T.Log(fmt.Sprintf("%s:[%s] %s", l.prefix, lvl, fmt.Sprintf(m, v...)))
}

type testCoordCtx struct {
	testLogger
	lost chan string
}

func newCtx(t *testing.T, prefix string) *testCoordCtx {
	return &testCoordCtx{
		testLogger: testLogger{prefix: prefix, T: t},
		lost:       make(chan string, 10),
	}
}

func (t *testCoordCtx) Lost(taskID string) {
	t.Log(metafora.LogLevelDebug, "Lost(%s)", taskID)
	t.lost <- taskID
}
