package m_etcd

import (
	"os"
	"strings"
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

func TestEtcdClientIntegration(t *testing.T) {
	skipEtcd(t)

	eclient := createEtcdClient(t)

	mclient := NewEtcdClient("test", eclient)

	if err := mclient.SubmitTask("testid1"); err != nil {
		t.Fatalf("Unable to submit task.   error:%v", err)
	}

	if err := mclient.SubmitTask("testid1"); err == nil {
		t.Fatalf("We shoudln't have been allowed to submit the same task twice.   error:%v", err)
	}
}

func createEtcdClient(t *testing.T) *etcd.Client {
	peerAddrs := os.Getenv("ETCDCTL_PEERS") //This is the same ENV that etcdctl uses for Peers.

	if peerAddrs == "" {
		peerAddrs = defaultPeers
	}

	peers := strings.Split(peerAddrs, ",")

	client := etcd.NewClient(peers)

	ok := client.SyncCluster()

	if !ok {
		t.Fatalf("Cannot sync with the cluster using peers " + strings.Join(peers, ", "))
	}

	if !isEtcdUp(client, t) {
		t.Fatalf("While testing etcd, the test couldn't connect to etcd. " + strings.Join(peers, ", "))
	}

	return client

}
