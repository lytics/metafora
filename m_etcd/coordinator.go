package m_etcd

import (
	"encoding/json"
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
	isClosed     bool
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
		if err == etcd.ErrWatchStoppedByUser {
			// This isn't actually an error, return nil
			err = nil
		}
		w.errorChan <- err
		close(w.errorChan)
	}()
}

// Close stops the watching goroutine. Close will panic if called more than
// once.
func (w *EtcdWatcher) Close() error {
	if w.isClosed {
		close(w.stopChan)
		w.isClosed = true
		return <-w.errorChan
	}

	return nil
}

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	Namespace string
	TaskPath  string

	taskWatcher       *EtcdWatcher
	tasksPrefetchChan chan *etcd.Node

	NodeID         string
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
		nodeId = hn + uuid.NewRandom().String()
	}

	nodeId = strings.Trim(nodeId, "/ ")

	etcd := &EtcdCoordinator{
		Client:    client,
		Namespace: namespace,

		TaskPath:    fmt.Sprintf("/%s/%s", namespace, TasksPath), //TODO MAKE A PACKAGE FUNC TO CREATE THIS PATH.
		taskWatcher: nil,

		NodeID:         nodeId,
		CommandPath:    fmt.Sprintf("/%s/%s/%s/%s", namespace, NodesPath, nodeId, CommandsPath),
		commandWatcher: nil,
	}

	return etcd
}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) {
	ec.Client.SetConsistency(etcd.STRONG_CONSISTENCY)

	cordCtx.Log(metafora.LogLevelDebug, "namespace[%s] Etcd-Cluster-Peers[%s]", ec.Namespace,
		strings.Join(ec.Client.GetCluster(), ", "))

	ec.cordCtx = cordCtx

	ec.upsertDir("/"+ec.Namespace, ForeverTTL)
	ec.upsertDir(ec.TaskPath, ForeverTTL)
	//TODO setup node Dir with a shorter TTL and patch in the heartbeat to update the TTL
	ec.upsertDir(ec.CommandPath, ForeverTTL)

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
		//Read the tasks
		const sorted = false
		const recursive = true
		resp, err := ec.Client.Get(ec.TaskPath, sorted, recursive)
		if err != nil {
			cordCtx.Log(metafora.LogLevelDebug, "Init error getting the current tasks from the path:[%s] error:[%v]", ec.TaskPath, err)
		} else {
			//Now dump any current tasks
			cordCtx.Log(metafora.LogLevelDebug, "Fetching tasks, response from GET %s: response[action:%v etcdIndex:%v nodeKey:%v]",
				ec.TaskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)

			for _, node := range resp.Node.Nodes {
				ec.tasksPrefetchChan <- node
			}
		}

		//After processing the existing tasks, lets start up the background watcher() to look for new ones.
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

func (ec *EtcdCoordinator) upsertDir(path string, ttl uint64) {

	//hidden etcd key that isn't visible to ls commands on the directory,
	//  you have to know about it to find it :).  I'm using it to add some
	//  info about when the cluster's schema was setup.
	pathMarker := path + "/" + MetadataKey
	const sorted = false
	const recursive = false

	_, err := ec.Client.Get(path, sorted, recursive)
	if err == nil {
		return
	}

	etcdErr, ok := err.(*etcd.EtcdError)
	if ok && etcdErr.ErrorCode == EcodeKeyNotFound {
		_, err := ec.Client.CreateDir(path, ttl)
		if err != nil {
			ec.cordCtx.Log(metafora.LogLevelDebug, "Error trying to create directory. path:[%s] error:[ %v ]", path, err)
		}
		host, _ := os.Hostname()

		metadata := struct {
			Host        string
			CreatedTime string
			NodeID      string
		}{
			host,
			time.Now().String(),
			ec.NodeID,
		}
		metadataB, _ := json.Marshal(metadata)
		metadataStr := string(metadataB)
		ec.Client.Create(pathMarker, metadataStr, ttl)
	}
}

// Watch will do a blocking etcd watch() on taskPath until a taskId is returned.
// The return taskId isn't guaranteed to be claimable.
//
func (ec *EtcdCoordinator) Watch() (taskID string, err error) {

	for {
		select {
		// before the watcher starts, this channel is filled up with existing tasks during init()
		case node, ok := <-ec.tasksPrefetchChan:
			if !ok {
				return "", nil
			}
			ec.cordCtx.Log(metafora.LogLevelDebug, "New node from the tasksPrefetchChan %s: node[nodeKey:%v]",
				ec.TaskPath, node.Key)

			taskId, skip := ec.parseTaskIDFromNode(node)
			if !skip {
				return taskId, nil
			}
		case resp, ok := <-ec.taskWatcher.responseChan:
			if !ok {
				return "", nil
			}
			ec.cordCtx.Log(metafora.LogLevelDebug, "New response from watching %s: response[action:%v etcdIndex:%v nodeKey:%v]",
				ec.TaskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)

			if resp.Action != "create" { //TODO eventually we'll need to deal with expired "owner" nodes.
				continue
			}

			taskId, skip := ec.parseTaskIDFromNode(resp.Node)
			if !skip {
				return taskId, nil
			}

		case err, ok := <-ec.taskWatcher.errorChan:
			if !ok {
				return "", nil
			}
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
	//TODO per discussion last night, we are going to prefix tasks in the client,
	//     so we can easily determine tasks from other keys/dirs created in the
	//     tasks path.
	if strings.Contains(node.Key, OwnerMarker) {
		return "", true
	}

	ec.cordCtx.Log(metafora.LogLevelDebug, "Node key:%s", node.Key)
	taskpath := strings.Split(node.Key, "/")
	if len(taskpath) == 0 {
		//TODO log
		return "", true
	}
	if !node.Dir {
		return "", true
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
	res, err := ec.Client.CreateDir(key, ClaimTTL)
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
