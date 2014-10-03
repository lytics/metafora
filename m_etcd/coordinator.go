package m_etcd

import (
	"fmt"
	"os"
	"strings"

	"code.google.com/p/go-uuid/uuid"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

const (
	TASKS_PATH   = "tasks"
	COMMAND_PATH = "commands"
	NODES_PATH   = "nodes"
	CLAIM_TTL    = uint64(120) //seconds
)

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	Namespace string
	TaskPath  string

	taskWatcher *EtcdWatcher

	NodeId         string
	CommandPath    string
	commandWatcher *EtcdWatcher
}

type Watcher func(cordCtx metafora.CoordinatorContext)
type EtcdWatcher struct {
	cordCtx      metafora.CoordinatorContext
	path         string
	responseChan chan *etcd.Response
	stopChan     chan bool
	errorChan    chan error
	client       *etcd.Client
}

func (w *EtcdWatcher) StartWatching() {
	go func() {
		const recursive = true
		const raftIndex = uint64(0) //0 == latest version

		//TODO, Im guessing that watch only picks up new nodes.
		//   We need to manually do an ls at startup and dump the results to taskWatcherResponses,
		//   after we filter out all non-claimed tasks.
		_, err := w.client.Watch(
			w.path,
			raftIndex,
			recursive,
			w.responseChan,
			w.stopChan)
		w.errorChan <- err
	}()
}

func (w *EtcdWatcher) Close() error {
	w.stopChan <- true

	select {
	case err := <-w.errorChan:
		return err
	default:
	}
	return nil
}

/*
Etcd paths:
/{namespace}/tasks/{task_id}/
/{namespace}/tasks/{task_id}/owner/


/{namespace}/node/{node_id}/command

TODO do we need a EtcdCoordinatorConfig?

*/
func NewEtcdCoordinator(nodeId, namespace string, client *etcd.Client) metafora.Coordinator {
	namespace = strings.Trim(namespace, "/ ")

	if nodeId == "" {
		hn, _ := os.Hostname()
		//Adding the UUID incase we run two nodes on the same box.
		// TODO lets move this to the Readme as part of the example of calling NewEtcdCoordinator.
		// Then just remove the Autocreated nodeId.
		nodeId = hn + string(uuid.NewRandom())
	}

	nodeId = strings.Trim(nodeId, "/ ")

	etcd := &EtcdCoordinator{
		Client:    client,
		Namespace: namespace,

		TaskPath:    fmt.Sprintf("/%s/%s", namespace, TASKS_PATH),
		taskWatcher: nil,

		NodeId:         nodeId,
		CommandPath:    fmt.Sprintf("/%s/%s/%s/%s", namespace, NODES_PATH, nodeId, COMMAND_PATH),
		commandWatcher: nil,
	}

	return etcd
}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) {

	ec.cordCtx = cordCtx
	ec.taskWatcher = &EtcdWatcher{
		cordCtx:      cordCtx,
		path:         ec.TaskPath,
		responseChan: make(chan *etcd.Response),
		stopChan:     make(chan bool),
		errorChan:    make(chan error),
		client:       ec.Client,
	}

	ec.commandWatcher = &EtcdWatcher{
		cordCtx:      cordCtx,
		path:         ec.CommandPath,
		responseChan: make(chan *etcd.Response),
		stopChan:     make(chan bool),
		errorChan:    make(chan error),
		client:       ec.Client,
	}
	//start up the watchers.
	//  Doing this in Init() incase we need access to the Ctx object...
	ec.taskWatcher.StartWatching()
	ec.commandWatcher.StartWatching()
}

// Watch should do a blocking watch on the broker and return a task ID that
// can be claimed.
func (ec *EtcdCoordinator) Watch() (taskID string, err error) {
	for {
		select {
		case resp := <-ec.taskWatcher.responseChan:
			taskId := ""
			ec.cordCtx.Log(metafora.LogLevelDebug, "New response from %s, res %v", ec.TaskPath, resp)
			if resp.Action != "create" {
				continue
			}
			if resp.Node == nil {
				//TODO log
				continue
			}
			taskpath := strings.Split(resp.Node.Key, "/")
			if len(taskpath) == 0 {
				//TODO log
				continue
			}
			if !resp.Node.Dir {
				ec.cordCtx.Log(metafora.LogLevelWarn, "TaskID node shouldn't be a directory but a key.")
			}
			taskId = taskpath[len(taskpath)-1]

			return taskId, nil
		case err := <-ec.taskWatcher.errorChan:
			return "", err
		}
	}
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if another consumer has already
// claimed the ID.
func (ec *EtcdCoordinator) Claim(taskID string) bool {
	key := fmt.Sprintf("%s/%s/owner", ec.TaskPath, taskID)
	res, err := ec.Client.CreateDir(key, CLAIM_TTL)
	if err != nil {
		ec.cordCtx.Log(metafora.LogLevelDebug, "Claim failed: err %v", err)
		return false
	}
	ec.cordCtx.Log(metafora.LogLevelDebug, "Claim successful: resp %v", res)
	return true
}

func (ec *EtcdCoordinator) Release(taskID string) {
	key := fmt.Sprintf("%s/%s/owner", ec.TaskPath, taskID)
	_, err := ec.Client.DeleteDir(key)
	if err != nil {
		//TODO Pause and retry?!
		ec.cordCtx.Log(metafora.LogLevelError, "Release failed: %v", err)
	}
}

// Command blocks until a command for this node is received from the broker
// by the coordinator.
func (ec *EtcdCoordinator) Command() (cmd string, err error) {

	return "", nil
	/*  TODO 1) cleanup the log here to match that of Watch
	         2) discuss the schema for the command channel...
	         3) Add code to the close method.
	select {
	case resp := <-ec.commandWatcherResponses:
		taskId := ""
		if resp.Node.Dir {
			taskId = resp.Node.Key
		} else {
			ec.cordCtx.Log(LogLevelWarning, "Command node needs to be a dir, not a key.")
			taskId = resp.Node.Key
		}
		return taskId, nil
	case err := <-ec.commandWatcherErrs:
		return "", err
	}
	*/
}

func (ec *EtcdCoordinator) Close() error {
	ec.taskWatcher.Close()
	ec.commandWatcher.Close()
	return nil
}
