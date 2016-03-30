package m_etcd

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd/testutil"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	metafora.SetLogger(log.New(os.Stderr, "", log.Lmicroseconds|log.Lshortfile))
}

type testctx struct {
	Coord      *EtcdCoordinator
	Conf       *Config
	EtcdClient client.KeysAPI
	MClient    metafora.Client
	Cleanup    func()
}

// setupEtcd should be used for all etcd integration tests. It handles the following tasks:
//  * Skip tests if ETCDTESTS is unset
//  * Create and return an etcd client
//  * Create and return an initial etcd coordinator
//  * Clearing the test namespace in etcd
//
// Callers should call Cleanup() in a defer statement to avoid filling etcd
// with test namespaces.
func setupEtcd(t *testing.T) *testctx {
	ctx := &testctx{}
	c, etcdconf := testutil.NewEtcdClient(t)
	ctx.EtcdClient = c

	// Create a unique namespace
	testid := strconv.FormatInt(rand.Int63(), 36)
	ns := "metafora-test-ns-" + testid

	// Cleanup before and after tests
	ctx.Cleanup = func() { c.Delete(context.TODO(), ns, &client.DeleteOptions{Recursive: true, Dir: true}) }
	ctx.Cleanup()

	// Create a coordinator config
	ctx.Conf = NewConfig("metafora-test-node-"+testid, ns, etcdconf.Endpoints)
	ctx.Conf.EtcdConfig = etcdconf

	coord, err := NewEtcdCoordinator(ctx.Conf)
	if err != nil {
		t.Fatalf("Error creating etcd coordinator: %v", err)
	}
	ctx.Coord = coord
	ctx.MClient = NewClientFromConfig(ctx.Conf)
	return ctx
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
