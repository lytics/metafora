package metafora

import (
	"fmt"

	"github.com/coreos/go-etcd/etcd"
)

const (
	TASKS_PATH = "tasks"
)

type EtcdCoordinator struct {
	Client               *etcd.Client
	Namespace            string
	TaskPath             string
	taskWatcherResponses chan *etcd.Response
	taskWatcherErrs      chan error
	taskWatcherStopper   chan bool
}

func NewEtcCoordinator(namespace string, client *etcd.Client) {

	etcd := &EtcdCoordinator{
		Client:               client,
		Namespace:            namespace,
		TaskPath:             fmt.Sprintf("/%s/%s", namespace, TASK_PATH),
		taskWatcherResponses: make(chan *etcd.Response),
		taskWatcherErrs:      make(chan error, 1),
		taskWatcherStopper:   make(chan bool),
	}

	etcdWatcher := func() {
		_, err := client.Watch(etcd.TaskPath, uint64(0) /*get latest version*/, false /*not recursive*/, etcd.taskWatcherResponses, etcd.taskWatcherStopper)
		etcd.taskWatcherErrs <- err
	}()
}

// Watch should do a blocking watch on the broker and return a task ID that
// can be claimed.
func Watch() (taskID string, err error) {
	select {
	case resp := <-etcd.taskWatcherResponses:
		taskId := ""
		if resp.Node.Dir {
			taskId = resp.Node.Key
		} else {
			//TODO: log an error that they taskID node needs to be a dir, not a key.
			taskId = resp.Node.Key
		}
		return taskId, nil
	case err := <-etcd.taskWatcherErrs:
		return nil, err
	}
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if another consumer has already
// claimed the ID.
func Claim(taskID string) (bool, error) {

}

// Command blocks until a command for this node is received from the broker
// by the coordinator.
func Command() (cmd string, err error) {

}
