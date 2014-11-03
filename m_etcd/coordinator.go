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

type ownerValue struct {
	Node string `json:"node"`
}

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

	var index uint64

	// Get existing tasks
	const sorted = true
	const recursive = true
	resp, err := w.client.Get(w.path, sorted, recursive)
	if err != nil {
		w.cordCtx.Log(metafora.LogLevelError, "Error getting the existing tasks from the path:%s error:%v", w.path, err)
		goto done
	}

	for _, node := range resp.Node.Nodes {
		// Act like these are newly created nodes
		select {
		case <-w.stopChan:
			goto done
		case w.responseChan <- &etcd.Response{Action: "create", Node: node, EtcdIndex: resp.EtcdIndex}:
		}
	}

	index = resp.EtcdIndex

	// Start blocking watch
	for {
		// Make a new inner response channel on each loop since Watch() closes the
		// one passed in.
		innerRespChan := make(chan *etcd.Response)
		go func() {
			for {
				select {
				case r, ok := <-innerRespChan:
					if !ok {
						// Don't close w.responseChan when the Watch exits
						return
					}
					// Update the last seen index
					index = r.EtcdIndex
					w.responseChan <- r
				case <-w.stopChan:
				}
			}
		}()
		// Start the blocking watch.
		_, err = w.client.Watch(w.path, index, recursive, innerRespChan, w.stopChan)
		if err != nil {
			if err == etcd.ErrWatchStoppedByUser {
				// This isn't actually an error, return nil
				err = nil
			} else if jsonErr, ok := err.(*json.SyntaxError); ok && jsonErr.Offset == 0 {
				// This is a bug in Go's HTTP transport + go-etcd which causes the
				// connection to timeout perdiocally and need to be restarted *after*
				// closing idle connections.
				w.cordCtx.Log(metafora.LogLevelDebug, "Watch timed out; restarting")
				transport.CloseIdleConnections()
				err = nil
				continue
			}
		}
		goto done
	}
done:
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

// stops the watching goroutine.
func (w *watcher) stop() {
	if w.watching() {
		select {
		case <-w.stopChan:
			// already stopped, let's avoid panic()s
		default:
			close(w.stopChan)
		}
	}
}

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	Namespace string
	TaskPath  string

	taskWatcher *watcher
	ClaimTTL    uint64 // seconds

	NodeID         string
	CommandPath    string
	commandWatcher *watcher

	taskManager *taskManager
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

	client.SetTransport(transport)
	client.SetConsistency(etcd.STRONG_CONSISTENCY)
	return &EtcdCoordinator{
		Client:    client,
		Namespace: namespace,

		TaskPath: fmt.Sprintf("/%s/%s", namespace, TasksPath),
		ClaimTTL: ClaimTTL, //default to the package constant, but allow it to be overwritten

		NodeID:      nodeId,
		CommandPath: fmt.Sprintf("/%s/%s/%s/%s", namespace, NodesPath, nodeId, CommandsPath),
	}
}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) {
	cordCtx.Log(metafora.LogLevelDebug, "Initializing coordinator with namespace: %s and etcd cluster: %s",
		ec.Namespace, strings.Join(ec.Client.GetCluster(), ", "))

	ec.cordCtx = cordCtx

	ec.upsertDir("/"+ec.Namespace, ForeverTTL)
	ec.upsertDir(ec.TaskPath, ForeverTTL)
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

	ec.taskManager = newManager(cordCtx, ec.Client, ec.TaskPath, ec.NodeID, ec.ClaimTTL)
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
			Host        string `json:"host"`
			CreatedTime string `json:"created"`
			ownerValue
		}{
			Host:        host,
			CreatedTime: time.Now().String(),
			ownerValue:  ownerValue{Node: ec.NodeID},
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

			//The etcd watcher may have received a new task signal.
			if resp.Action == "create" {
				ec.cordCtx.Log(metafora.LogLevelDebug, "New task signaled while watching %s: response[action:%v etcdIndex:%v nodeKey:%v]",
					ec.TaskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)
				taskId, skip := ec.parseTaskIdFromTaskNode(resp.Node)
				if !skip {
					return taskId, nil
				}
			}

			//The etcd watcher may have received a released task or an owner may have left the cluster.
			//Ref: etcd event types : https://github.com/coreos/etcd/blob/master/store/event.go#L4
			if (resp.Action == "expire" || resp.Action == "delete" || resp.Action == "compareAndDelete") &&
				ec.nodeIsTheOwnerMarker(resp.Node) {

				ec.cordCtx.Log(metafora.LogLevelDebug, "Released task signaled while watching %s: response[action:%v etcdIndex:%v nodeKey:%v]",
					ec.TaskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)

				taskId, skip := ec.parseTaskIdFromOwnerNode(resp.Node.Key)
				if !skip {
					return taskId, nil
				}
			}

		case err := <-ec.taskWatcher.errorChan:
			return "", err
		}
	}
}

func (ec *EtcdCoordinator) parseTaskIdFromOwnerNode(nodePath string) (taskID string, skip bool) {
	//remove ec.TaskPath
	res := strings.Replace(nodePath, ec.TaskPath+"/", "", 1)
	//remove OwnerMarker
	res2 := strings.Replace(res, "/"+OwnerMarker, "", 1)
	//if remainder doens't contain "/", then we've found the taskid
	if !strings.Contains(res2, "/") {
		return res2, false
	} else {
		return "", true
	}
}

//Determines if its a task by removing the [ec.TaskPath + "/"] from the path
// and if there are still slashes then we know its a child of the the task
// not the task it's self.
func (ec *EtcdCoordinator) isATaskNode(nodePath string) bool {
	possibleTaskId := strings.Replace(nodePath, ec.TaskPath+"/", "", 1)
	if strings.Contains(possibleTaskId, "/") {
		return false
	} else {
		return true
	}
}

func (ec *EtcdCoordinator) parseTaskIdFromTaskNode(node *etcd.Node) (taskID string, skip bool) {
	taskId := ""

	if !ec.isATaskNode(node.Key) {
		return "", true
	}

	if ec.nodeHasOwnerMarker(node) {
		return "", true
	}

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

	ec.cordCtx.Log(metafora.LogLevelDebug, "A claimable task was found. task:%s key:%s", taskId, node.Key)
	return taskId, false
}

func (ec *EtcdCoordinator) nodeIsTheOwnerMarker(node *etcd.Node) bool {
	//If the node is the owner marker, it most likely means the recursive watch picked
	// this node's creation up.
	if strings.Contains(node.Key, OwnerMarker) {
		return true
	}
	return false
}

func (ec *EtcdCoordinator) nodeHasOwnerMarker(node *etcd.Node) bool {
	//If its a task with an owner marker (Child), then most likely this node came from
	// the prefetch code which found an existing task.
	if node.Nodes != nil && len(node.Nodes) > 0 {
		for _, n := range node.Nodes {
			if strings.Contains(n.Key, OwnerMarker) {
				return true
			}
		}
	}
	return false
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if another consumer has already
// claimed the ID.
func (ec *EtcdCoordinator) Claim(taskID string) bool {
	return ec.taskManager.add(taskID)
}

// Release deletes the claim file.
func (ec *EtcdCoordinator) Release(taskID string) {
	const done = false
	ec.taskManager.remove(taskID, done)
}

// Done deletes the task.
func (ec *EtcdCoordinator) Done(taskID string) {
	const done = true
	ec.taskManager.remove(taskID, done)
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

			if strings.HasSuffix(resp.Node.Key, MetadataKey) {
				// Skip metadata marker
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

// Close stops the coordinator and causes blocking Watch and Command methods to
// return zero values.
func (ec *EtcdCoordinator) Close() {
	ec.taskWatcher.stop()
	ec.commandWatcher.stop()
	ec.taskManager.stop()
}
