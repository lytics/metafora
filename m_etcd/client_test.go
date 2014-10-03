package m_etcd

import (
	"os"
	"strings"
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

func TestEtcdClientIntegration(t *testing.T) {
	if os.Getenv("IntegrationTests") == "" {
		return
	}

	eclient := createEtcdClient(t)

	mclient := NewEtcdClient("test", eclient)

	if err := mclient.SubmitTask("testid1"); err != nil {
		t.Fatalf("Unable to submit task.   error:%v", err)
	}

	if err := mclient.SubmitTask("testid1"); err == nil {
		t.Fatalf("Unable to submit task.   error:%v", err)
	}
}

func createEtcdClient(t *testing.T) *etcd.Client {
	peers_from_environment := os.Getenv("ETCDCTL_PEERS") //This is the same ENV that etcdctl uses for Peers.

	if peers_from_environment == "" {
		peers_from_environment = "localhost:5001,localhost:5002,localhost:5003"
	}

	peers := strings.Split(peers_from_environment, ",")

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
