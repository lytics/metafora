package metafora

import (
	"fmt"
	"log"
	"os"

	"github.com/coreos/go-etcd/etcd"
)

const (
	TASKS_PATH   = "tasks"
	COMMAND_PATH = "commands"
	NODES_PATH   = "nodes"
	CLAIM_TTL    = 120 //seconds
)

type EtcdCoordinator struct {
	Client    *etcd.Client
	Namespace string
	TaskPath  string

	taskWatcherResponses chan *etcd.Response
	taskWatcherErrs      chan error
	taskWatcherStopper   chan bool
	TaskWatcher          *Watcher

	NodeId                  string
	CommandPath             string
	commandWatcherResponses chan *etcd.Response
	commandWatcherErrs      chan error
	commandWatcherStopper   chan bool
}

type Watcher func()
type EtcdWatcher struct {
	Watcher    Watcher
	HasStarted bool
}

/*
Etcd paths:
/{namespace}/tasks/{task_id}/
/{namespace}/tasks/{task_id}/owner/


/{namespace}/node/{node_id}/command

TODO do we need a EtcdCoordinatorConfig?

*/
func NewEtcdCoordinator(nodeId, namespace string, client *etcd.Client) {

	if nodeId == "" {
		os.Hostname() //TODO add a guid as a postfix, so we can run more than one node per host.
	}

	etcd := &EtcdCoordinator{
		Client:    client,
		Namespace: namespace,

		TaskPath:             fmt.Sprintf("/%s/%s", namespace, TASKS_PATH),
		taskWatcherResponses: make(chan *etcd.Response),
		taskWatcherErrs:      make(chan error, 1),
		taskWatcherStopper:   make(chan bool),

		NodeId:                  nodeId,
		CommandPath:             fmt.Sprintf("/%s/%s/%s/%s", namespace, NODES_PATH, nodeId, COMMAND_PATH),
		commandWatcherResponses: make(chan *etcd.Response),
		commandWatcherErrs:      make(chan error, 1),
		commandWatcherStopper:   make(chan bool),
	}

	// Listens to a path like /{namespace}/tasks
	go func() {
		//TODO, Im guessing that watch only picks up new nodes.
		//   We need to manually do an ls at startup and dump the results to taskWatcherResponses,
		//   after we filter out all non-claimed tasks.
		_, err := client.Watch(
			etcd.TaskPath,
			uint64(0), /*get latest version*/
			false,     /*not recursive*/
			etcd.taskWatcherResponses,
			etcd.taskWatcherStopper)
		etcd.taskWatcherErrs <- err
	}()

	go func() {
		_, err := client.Watch(
			etcd.CommandPath,
			uint64(0), /*get latest version*/
			false,     /*not recursive*/
			etcd.commandWatcherResponses,
			etcd.commandWatcherStopper)
		etcd.commandWatcherErrs <- err
	}()
}

// Watch should do a blocking watch on the broker and return a task ID that
// can be claimed.
func (etcd *EtcdCoordinator) Watch() (taskID string, err error) {
	for {
		select {
		case resp := <-etcd.taskWatcherResponses:
			taskId := ""
			log.Printf("New response from %s, res %v", etcd.TaskPath, resp)
			if resp.Action != "create" {
				continue
			}

			if resp.Node.Dir {
				taskId = resp.Node.Key
			} else {
				//TODO: log an error that they taskID node needs to be a dir, not a key.
				log.Println("TaskID node needs to be a dir, not a key.")
				taskId = resp.Node.Key
			}
			return taskId, nil
		case err := <-etcd.taskWatcherErrs:
			return "", err
		}
	}
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if another consumer has already
// claimed the ID.
func (etcd *EtcdCoordinator) Claim(taskID string) (bool, error) {
	key := fmt.Sprintf("%s/%s/owner", etcd.TaskPath, taskID)
	res, err := etcd.Client.CreateDir(key, uint64(CLAIM_TTL))
	if err != nil {
		log.Printf("Claim failed: err %v", err)
		return false, err
	}
	log.Printf("Claim successful: resp %v", res)
	return true, nil
}

// Command blocks until a command for this node is received from the broker
// by the coordinator.
func (etcd *EtcdCoordinator) Command() (cmd string, err error) {
	select {
	case resp := <-etcd.commandWatcherResponses:
		taskId := ""
		if resp.Node.Dir {
			taskId = resp.Node.Key
		} else {
			//TODO: log an error that they taskID node needs to be a dir, not a key.
			log.Println("Command node needs to be a dir, not a key.")
			taskId = resp.Node.Key
		}
		return taskId, nil
	case err := <-etcd.commandWatcherErrs:
		return "", err
	}
}

func (etcd *EtcdCoordinator) Close() error {
	etcd.taskWatcherStopper <- true

	select {
	case err := <-etcd.taskWatcherErrs:
		return err
	default:
	}

	return nil
}
