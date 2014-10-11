package m_etcd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type watcher struct {
	cordCtx      metafora.CoordinatorContext
	path         string
	responseChan chan *etcd.Response // coordinator watches for changes
	errorChan    chan error          // coordinator watches for errors
	stopChan     chan bool           // closed to signal etcd's Watch to stop
	client       *etcd.Client
	running      bool       // only set in watch(); only read by watching()
	m            sync.Mutex // running requires synchronization
}

// watch emits etcd responses over responseChan until stopChan is closed.
func (w *watcher) watch() {
	// Setup watch state
	w.m.Lock()
	w.running = true
	w.stopChan = make(chan bool)
	w.m.Unlock()

	// Teardown watch state when watch() returns
	defer func() {
		w.m.Lock()
		w.running = false
		w.m.Unlock()
	}()

	// Get existing tasks
	const sorted = true
	const recursive = true
	resp, err := w.client.Get(w.path, sorted, recursive)
	if err != nil {
		w.cordCtx.Log(metafora.LogLevelError, "Error getting the existing tasks from the path:%s error:%v", w.path, err)
		w.errorChan <- err
		return
	}

	for _, node := range resp.Node.Nodes {
		// Act like these are newly created nodes
		w.responseChan <- &etcd.Response{Action: "create", Node: node, EtcdIndex: resp.EtcdIndex}
	}

	// Start blocking watch
	_, err = w.client.Watch(w.path, resp.EtcdIndex, recursive, w.responseChan, w.stopChan)
	if err == etcd.ErrWatchStoppedByUser {
		// This isn't actually an error, return nil
		err = nil
	}
	w.errorChan <- err
}

// watching is a safe way for concurrent goroutines to check if the watcher is
// running.
func (w *watcher) watching() bool {
	w.m.Lock()
	r := w.running
	w.m.Unlock()
	return r
}

// Close stops the watching goroutine. Close will panic if called more than
// once.
func (w *watcher) stop() {
	if w.watching() {
		close(w.stopChan)
	}
}

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	Namespace string
	TaskPath  string

	taskWatcher *watcher

	NodeID         string
	CommandPath    string
	commandWatcher *watcher
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

	return &EtcdCoordinator{
		Client:    client,
		Namespace: namespace,

		TaskPath: fmt.Sprintf("/%s/%s", namespace, TasksPath), //TODO MAKE A PACKAGE FUNC TO CREATE THIS PATH.

		NodeID:      nodeId,
		CommandPath: fmt.Sprintf("/%s/%s/%s/%s", namespace, NodesPath, nodeId, CommandsPath),
	}
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

	ec.taskWatcher = &watcher{
		cordCtx:      cordCtx,
		path:         ec.TaskPath,
		responseChan: make(chan *etcd.Response),
		errorChan:    make(chan error),
		client:       ec.Client,
	}

	ec.commandWatcher = &watcher{
		cordCtx:      cordCtx,
		path:         ec.CommandPath,
		responseChan: make(chan *etcd.Response),
		errorChan:    make(chan error),
		client:       ec.Client,
	}
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
	if !ec.taskWatcher.watching() {
		// Watcher hasn't been started, so start it now
		go ec.taskWatcher.watch()
	}

	for {
		select {
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

		case err := <-ec.taskWatcher.errorChan:
			return "", err
		}
	}
}

func (ec *EtcdCoordinator) parseTaskIDFromNode(node *etcd.Node) (taskID string, skip bool) {
	taskId := ""

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
func (ec *EtcdCoordinator) Command() (metafora.Command, error) {
	if !ec.commandWatcher.watching() {
		go ec.commandWatcher.watch()
	}

	for {
		select {
		case resp, ok := <-ec.commandWatcher.responseChan:
			if !ok {
				return nil, nil
			}

			if resp.Action != "create" {
				continue
			}

			const recurse = false
			if _, err := ec.Client.Delete(resp.Node.Key, recurse); err != nil {
				ec.cordCtx.Log(metafora.LogLevelError, "Error deleting handled command %s: %v", resp.Node.Key, err)
			}

			return metafora.UnmarshalCommand([]byte(resp.Node.Value))
		case err := <-ec.commandWatcher.errorChan:
			return nil, err
		}
	}
}

func (ec *EtcdCoordinator) Freeze() {
	ec.taskWatcher.stop()
}

func (ec *EtcdCoordinator) Close() {
	ec.taskWatcher.stop()
	ec.commandWatcher.stop()
}
