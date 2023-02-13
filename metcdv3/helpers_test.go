package metcdv3

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/metcdv3/testutil"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

func init() {
	metafora.SetLogger(log.New(os.Stderr, "", log.Lmicroseconds|log.Lshortfile))
	//metafora.SetLogLevel(metafora.LogLevelDebug)
}

var testcounter uint64

// setupEtcd should be used for all etcd integration tests. It handles the following tasks:
//   - Create and return an etcd client
//   - Create and return an initial etcd coordinator
//   - Clearing the test namespace in etcd
func setupEtcd(t *testing.T) (*etcdv3.Client, *EtcdV3Coordinator, *Config) {
	c := context.Background()
	client := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(client)
	n := atomic.AddUint64(&testcounter, 1)
	ns := fmt.Sprintf("/metaforatests-%d", n)
	_, err := kvc.Delete(c, ns, etcdv3.WithPrefix())
	if err != nil {
		t.Errorf("failed to clean up namespace in etcd")
	}
	conf := NewConfig("testclient", ns)
	coord := NewEtcdV3Coordinator(conf, client)
	return client, coord, conf
}

type testLogger struct {
	prefix string
	*testing.T
}

func (l testLogger) Log(lvl int, m string, v ...interface{}) {
	l.T.Log(fmt.Sprintf("%s:[%d] %s", l.prefix, lvl, fmt.Sprintf(m, v...)))
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
	t.Log(4, "Lost(%s)", task.ID())
	t.lost <- task.ID()
}
