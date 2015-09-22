package m_etcd

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd/testutil"
)

func init() {
	metafora.SetLogger(log.New(os.Stderr, "", log.Lmicroseconds|log.Lshortfile))
}

var testcounter uint64

// setupEtcd should be used for all etcd integration tests. It handles the following tasks:
//  * Skip tests if ETCDTESTS is unset
//  * Create and return an etcd client
//  * Create and return an initial etcd coordinator
//  * Clearing the test namespace in etcd
func setupEtcd(t *testing.T) (*EtcdCoordinator, *Config) {
	client, hosts := testutil.NewEtcdClient(t)
	n := atomic.AddUint64(&testcounter, 1)
	ns := fmt.Sprintf("metaforatests-%d", n)
	client.Delete(ns, recursive)
	conf := NewConfig("testclient", ns, hosts)
	coord, err := NewEtcdCoordinator(conf)
	if err != nil {
		t.Fatalf("Error creating etcd coordinator: %v", err)
	}
	return coord, conf
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

func (t *testCoordCtx) Lost(task metafora.Task) {
	t.Log(metafora.LogLevelDebug, "Lost(%s)", task.ID())
	t.lost <- task.ID()
}
