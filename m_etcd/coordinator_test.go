package m_etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type testLogger struct {
	*testing.T
}

func (l testLogger) Log(lvl metafora.LogLevel, m string, v ...interface{}) {
	l.T.Log(fmt.Sprintf("[%s] %s", lvl, fmt.Sprintf(m, v...)))
}

/*
	Running the Integration Test:
	#if you don't have etcd install use this script to set it up:
	sudo bash ./scripts/docker_run_etcd.sh

	ETCDTESTS=1 go test -v ./...
*/
func TestTaskWatcherEtcdCoordinatorIntegration(t *testing.T) {
	coordinator1, client := createEtcdCoordinator(t)

	if coordinator1.TaskPath != "/testcluster/tasks" {
		t.Fatalf("TestFailed: TaskPath should be \"/testcluster/tasks\" but we got \"%s\"", coordinator1.TaskPath)
	}

	coordinator1.Init(testLogger{t})

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

func createEtcdCoordinator(t *testing.T) (*EtcdCoordinator, *etcd.Client) {
	c := newEtcdClient(t)

	return NewEtcdCoordinator("test-1", "/testcluster/", c).(*EtcdCoordinator), c
}
