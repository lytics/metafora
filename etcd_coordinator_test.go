package metafora

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

/*
	Running the Integration Test:
	#if you don't have etcd install use this script to set it up:
	sudo bash ./scripts/docker_run_etcd.sh

	PUBLIC_IP=`hostname --ip-address` IntegrationTests=true ETCDCTL_PEERS="${PUBLIC_IP}:5001,${PUBLIC_IP}:5002,${PUBLIC_IP}:5003" go test

*/
func TestTaskWatcherEtcdCoordinatorIntegration(t *testing.T) {

	if os.Getenv("IntegrationTests") == "" {
		return
	}

	coordinator1, client := createEtcdCoordinator(t)

	if coordinator1.TaskPath != "/testcluster/tasks" {
		t.Fatalf("TestFailed: TaskPath should be \"/testcluster/tasks\" but we got \"%s\"", coordinator1.TaskPath)
	}

	coordinator1.Init(newBasicLogger())

	watchRes := make(chan string)
	task001 := "test-task0001"
	fullTask001Path := coordinator1.TaskPath + "/" + task001
	client.Delete(coordinator1.TaskPath+task001, true)
	go func() {
		//Watch blocks, so we need to test it in its own go routine.
		taskId, err := coordinator1.Watch()
		if err != nil {
			t.Fatalf("coordinator1.Watch() returned an err: %v", err)
		}
		t.Logf("We got a task id from the coordinator1.Watch() res:%s", taskId)
		watchRes <- taskId
	}()

	client.CreateDir(fullTask001Path, 1)

	select {
	case taskId := <-watchRes:
		if taskId != task001 {
			t.Fatalf("coordinator1.Watch() test failed: We received the incorrect taskId.  Got [%s] Expected[%s]", taskId, task001)
		}
	case <-time.After(time.Second * 15):
		t.Fatalf("coordinator1.Watch() test failed: The testcase timedout after 5 seconds.")
	}
}

func TestTaskClaimingEtcdCoordinatorIntegration(t *testing.T) {
	if os.Getenv("IntegrationTests") == "" {
		return
	}

	createEtcdCoordinator(t)
}

func createEtcdCoordinator(t *testing.T) (*EtcdCoordinator, *etcd.Client) {
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

	return NewEtcdCoordinator("test-1", "/testcluster/", client), client
}

func isEtcdUp(client *etcd.Client, t *testing.T) bool {
	client.Create("/foo", "test", 1)
	res, err := client.Get("/foo", false, false)
	if err != nil {
		t.Errorf("Writing a test key to etcd failed. error:%v", err)
		return false
	} else {
		t.Log(fmt.Sprintf("Res:[Action:%s Key:%s Value:%s tll:%d]", res.Action, res.Node.Key, res.Node.Value, res.Node.TTL))
		return true
	}
}
