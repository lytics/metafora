package m_etcd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

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
		const etcdIndex = uint64(0) //0 == latest version

		//TODO, Im guessing that watch only picks up new nodes.
		//   We need to manually do an ls at startup and dump the results to taskWatcherResponses,
		//   after we filter out all non-claimed tasks.
		_, err := w.client.Watch(
			w.path,
			etcdIndex,
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

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	Namespace string
	TaskPath  string

	taskWatcher       *EtcdWatcher
	tasksPrefetchChan chan *etcd.Node
	ClaimTTL          uint64

	NodeId         string
	NodePath       string
	CommandPath    string
	commandWatcher *EtcdWatcher
}

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
		ClaimTTL:  ClaimTTL,

		TaskPath:          fmt.Sprintf("/%s/%s", namespace, TasksPath), //TODO MAKE A PACKAGE FUNC TO CREATE THIS PATH.
		taskWatcher:       nil,
		tasksPrefetchChan: nil,

		NodeId:         nodeId,
		CommandPath:    fmt.Sprintf("/%s/%s/%s/%s", namespace, NodesPath, nodeId, CommandsPath),
		NodePath:       fmt.Sprintf("/%s/%s/%s/%s", namespace, NodesPath, nodeId),
		commandWatcher: nil,
	}

	return etcd
}

func (ec *EtcdCoordinator) upsertDir(path string, ttl uint64) {

	pathMarker := path + "/" + CreatedMarker //hidden etcd key that isn't visible to directory get commands

	const sorted = false
	const recursive = false
	_, err := ec.Client.Get(pathMarker, sorted, recursive)
	if err != nil && !strings.Contains(err.Error(), "Key not found") {
		ec.cordCtx.Log(metafora.LogLevelDebug, "Error trying to test for the existence of path. path:[%s] error:[ %v ]", path, err)
		return
	}
	if err != nil {
		_, err := ec.Client.CreateDir(path, ttl)
		if err != nil {
			ec.cordCtx.Log(metafora.LogLevelDebug, "Error trying to create directory. path:[%s] error:[ %v ]", path, err)
		}
		host, _ := os.Hostname()
		markerVal := fmt.Sprintf("createdAt=%s&by=%s", time.Now().String(), host)
		ec.Client.Set(pathMarker, markerVal, ttl) //You can't test for the existence of an empty dir, but _key's are hidden.
	}

}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) {

	cordCtx.Log(metafora.LogLevelDebug, "namespace[%s]", ec.Namespace)

	ec.cordCtx = cordCtx

	ec.upsertDir("/"+ec.Namespace, ForeverTTL)
	ec.upsertDir(ec.TaskPath, ForeverTTL)
	//ec.upsertDir(ec.NodePath, ForeverTTL)
	//ec.upsertDir(ec.CommandPath, ForeverTTL)

	ec.tasksPrefetchChan = make(chan *etcd.Node)
	ec.taskWatcher = &EtcdWatcher{
		cordCtx:      cordCtx,
		path:         ec.TaskPath,
		responseChan: make(chan *etcd.Response),
		stopChan:     make(chan bool),
		errorChan:    make(chan error),
		client:       ec.Client,
	}

	go func() {
		//First read the and dump the current tasks, then start up the background watcher()
		const sorted = false
		const recursive = true
		resp, err := ec.Client.Get(ec.TaskPath, sorted, recursive)

		if err != nil {
			cordCtx.Log(metafora.LogLevelDebug, "Init error getting the current tasks from the path:[%s] error:[%v]", ec.TaskPath, err)
		} else {

			cordCtx.Log(metafora.LogLevelDebug, "Fetching tasks, response from GET %s: response[action:%v etcdIndex:%v nodeKey:%v]",
				ec.TaskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)

			for _, node := range resp.Node.Nodes {
				cordCtx.Log(metafora.LogLevelDebug, "---"+node.Key)
				ec.tasksPrefetchChan <- node
			}
		}

		//Don't start up the watcher until we've iterated over the existing tasks
		ec.taskWatcher.StartWatching()
	}()

	ec.commandWatcher = &EtcdWatcher{
		cordCtx:      cordCtx,
		path:         ec.CommandPath,
		responseChan: make(chan *etcd.Response),
		stopChan:     make(chan bool),
		errorChan:    make(chan error),
		client:       ec.Client,
	}

	ec.commandWatcher.StartWatching()
}

// Watch should do a blocking watch on the broker and return a task ID that
// can be claimed.
func (ec *EtcdCoordinator) Watch() (taskID string, err error) {
	for {
		select {
		// before the watcher starts, this channel is filled up with existing tasks during init()
		case node := <-ec.tasksPrefetchChan:
			ec.cordCtx.Log(metafora.LogLevelDebug, "New node from the tasksPrefetchChan %s: node[nodeKey:%v]",
				ec.TaskPath, node.Key)

			taskId, skip := ec.parseTaskIDFromNode(node)
			if !skip {
				return taskId, nil
			}
		case resp := <-ec.taskWatcher.responseChan:
			ec.cordCtx.Log(metafora.LogLevelDebug, "New response from watching %s: response[action:%v etcdIndex:%v nodeKey:%v]",
				ec.TaskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)

			if resp.Action != "create" { //TODO eventually we'll need to deal with expired "owner" nodes.
				continue
			}

			taskId, skip := ec.parseTaskIDFromNode(resp.Node)
			if !skip {
				return taskId, nil
			}
		case err := <-ec.taskWatcher.errorChan:
			//if the watcher sends a nil, its because the etcd watcher was shutdown by Close()
			return "", err
		}
	}
}

func (ec *EtcdCoordinator) parseTaskIDFromNode(node *etcd.Node) (taskID string, skip bool) {
	taskId := ""

	if node == nil {
		//TODO log
		return "", true
	}

	//FIXME There's gotta be a better way to only detect tasks #32
	if strings.Contains(node.Key, OwnerMarker) || strings.Contains(node.Key, CreatedMarker) {
		return "", true
	}

	ec.cordCtx.Log(metafora.LogLevelDebug, "Node key:%s", node.Key)
	taskpath := strings.Split(node.Key, "/")
	if len(taskpath) == 0 {
		//TODO log
		return "", true
	}
	if !node.Dir {
		ec.cordCtx.Log(metafora.LogLevelWarn, "TaskID node shouldn't be a directory but a key.")
	}

	taskId = taskpath[len(taskpath)-1]

	if taskId == "tasks" {
		return "", true
	}

	return taskId, false
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if another consumer has already
// claimed the ID.
func (ec *EtcdCoordinator) Claim(taskID string) bool {
	key := fmt.Sprintf("%s/%s/%s", ec.TaskPath, taskID, OwnerMarker)
	res, err := ec.Client.CreateDir(key, ec.ClaimTTL)
	if err != nil {
		ec.cordCtx.Log(metafora.LogLevelDebug, "Claim failed: err %v", err)
		return false
	}
	ec.cordCtx.Log(metafora.LogLevelDebug, "Claim successful: resp %v", res)
	return true
}

// Release deletes the claim directory.
func (ec *EtcdCoordinator) Release(taskID string) {
	key := fmt.Sprintf("%s/%s/%s", ec.TaskPath, taskID, OwnerMarker)
	//FIXME Conditionally delete only if this node is actually the owner
	_, err := ec.Client.DeleteDir(key)
	if err != nil {
		//TODO Pause and retry?!
		ec.cordCtx.Log(metafora.LogLevelError, "Release failed: %v", err)
	}
}

// Command blocks until a command for this node is received from the broker
// by the coordinator.
func (ec *EtcdCoordinator) Command() (cmd string, err error) {
	<-ec.commandWatcher.responseChan
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
