package m_etcd

import (
	"fmt"
	"testing"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd/testutil"
)

const (
	namespace = "/metaforatests"
	nodeID    = "node1"
)

// setupEtcd should be used for all etcd integration tests. It handles the following tasks:
//  * Skip tests if ETCDTESTS is unset
//  * Create and return an etcd client
//  * Create and return an initial etcd coordinator
//  * Clearing the test namespace in etcd
func setupEtcd(t *testing.T) (*EtcdCoordinator, []string) {
	client, hosts := testutil.NewEtcdClient(t)
	client.Delete(namespace, recursive)
	coord, err := NewEtcdCoordinator(nodeID, namespace, hosts)
	if err != nil {
		t.Fatalf("Error creating etcd coordinator: %v", err)
	}
	return coord.(*EtcdCoordinator), hosts
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
