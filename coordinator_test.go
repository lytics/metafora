package metafora

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/coreos/go-etcd/etcd"
)

func TestConsumer(t *testing.T) {
	peers_from_environment := os.Getenv("ETCDCTL_PEERS") //This is the same ENV that etcdctl uses for Peers.
	if peers_from_environment == "" {
		peers_from_environment = "localhost:5001,localhost:5002,localhost:5003"
	}

	peers := strings.Split(peers_from_environment, ",")

	client := etcd.NewClient(peers)

	ok := client.SyncCluster()

	if !ok {
		t.Errorf("Cannot sync with the cluster using peers " + strings.Join(peers, ", "))
		return
	}

	res, err := client.Get("/foo", false, false)
	if err != nil {
		t.Errorf("Cannot sync with the cluster using peers %s error:%v", strings.Join(peers, ", "), err)
		return
	} else {
		fmt.Println(fmt.Sprintf("%v", res))
	}
}
