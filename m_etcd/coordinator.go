package m_etcd

import (
	"encoding/json"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

var DefaultNodePathTTL uint64 = 10 // seconds

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

	var err error

	// Teardown watch state when watch() returns and send err
	defer func() {
		w.m.Lock()
		w.running = false
		w.m.Unlock()
		w.errorChan <- err
	}()

	// Get existing tasks
	const sorted = true
	const recursive = true
	var resp *etcd.Response
	resp, err = w.client.Get(w.path, sorted, recursive)
	if err != nil {
		w.cordCtx.Log(metafora.LogLevelError, "Error getting the existing tasks from the path:%s error:%v", w.path, err)
		return
	}

	for _, node := range resp.Node.Nodes {
		// Act like these are newly created nodes
		select {
		case <-w.stopChan:
			return
		case w.responseChan <- &etcd.Response{Action: "create", Node: node, EtcdIndex: resp.EtcdIndex}:
		}
	}

	var rawResp *etcd.RawResponse

	// Start blocking watch
	for {
		// Start the blocking watch from the last response's index.
		w.cordCtx.Log(metafora.LogLevelDebug, "--> watch %v %v", w.path, resp.Node.ModifiedIndex+1)
		rawResp, err = w.client.RawWatch(w.path, resp.EtcdIndex, recursive, nil, w.stopChan)
		w.cordCtx.Log(metafora.LogLevelDebug, "<-- watch\n%s\n%+v %+v", string(rawResp.Body), rawResp.Header, err)
		if err != nil {
			if err == etcd.ErrWatchStoppedByUser {
				// This isn't actually an error, the stopChan was closed. Time to stop!
				err = nil
				return
			} else {
				// RawWatch errors should be treated as fatal since it internally
				// retries on network problems, and recoverable Etcd errors aren't
				// parsed until rawResp.Unmarshal is called later.
				w.cordCtx.Log(metafora.LogLevelError, "Unrecoverable watch error: %v", err)
				return
			}
		}

		select {
		case <-w.stopChan:
			return // stopped, exit now
		default:
		}

		if len(rawResp.Body) == 0 {
			// This is a bug in Go's HTTP transport + go-etcd which causes the
			// connection to timeout perdiocally and need to be restarted *after*
			// closing idle connections.
			w.cordCtx.Log(metafora.LogLevelDebug, "Watch response empty; restarting watch. Status: %d Headers: %+v",
				rawResp.StatusCode, rawResp.Header)
			transport.CloseIdleConnections()
			continue
		}

		if resp, err = rawResp.Unmarshal(); err != nil {
			w.cordCtx.Log(metafora.LogLevelError, "Unexpected error unmarshalling etcd response: %+v", err)
			return
		}

		select {
		case w.responseChan <- resp:
		case <-w.stopChan:
			return
		}
	}
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
	namespace string
	taskPath  string

	taskWatcher *watcher
	ClaimTTL    uint64 // seconds

	NodeID         string
	nodePath       string
	nodePathTTL    uint64
	commandPath    string
	commandWatcher *watcher

	taskManager *taskManager

	// Close() sends nodeRefresher a chan to close when it has exited.
	stopNode chan struct{}
	closeL   sync.Mutex // only process one Close() call at once
	closed   bool
}

// NewEtcdCoordinator creates a new Metafora Coordinator implementation using
// etcd as the broker. If no node ID is specified, a unique one will be
// generated.
//
// Coordinator methods will be called by the core Metafora Consumer. Calling
// Init, Close, etc. from your own code will lead to undefined behavior.
func NewEtcdCoordinator(nodeID, namespace string, client *etcd.Client) metafora.Coordinator {
	// Namespace should be an absolute path with no trailing slash
	namespace = "/" + strings.Trim(namespace, "/ ")

	if nodeID == "" {
		hn, _ := os.Hostname()
		nodeID = hn + "-" + uuid.NewRandom().String()
	}

	nodeID = strings.Trim(nodeID, "/ ")

	client.SetTransport(transport)
	client.SetConsistency(etcd.STRONG_CONSISTENCY)
	return &EtcdCoordinator{
		Client:    client,
		namespace: namespace,

		taskPath: path.Join(namespace, TasksPath),
		ClaimTTL: ClaimTTL, //default to the package constant, but allow it to be overwritten

		NodeID:      nodeID,
		nodePath:    path.Join(namespace, NodesPath, nodeID),
		nodePathTTL: DefaultNodePathTTL,
		commandPath: path.Join(namespace, NodesPath, nodeID, CommandsPath),

		stopNode: make(chan struct{}),
	}
}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) error {
	cordCtx.Log(metafora.LogLevelDebug, "Initializing coordinator with namespace: %s and etcd cluster: %s",
		ec.namespace, strings.Join(ec.Client.GetCluster(), ", "))

	ec.cordCtx = cordCtx

	ec.upsertDir(ec.namespace, ForeverTTL)
	ec.upsertDir(ec.taskPath, ForeverTTL)
	if _, err := ec.Client.CreateDir(ec.nodePath, ec.nodePathTTL); err != nil {
		return err
	}
	go ec.nodeRefresher()
	ec.upsertDir(ec.commandPath, ForeverTTL)

	ec.taskWatcher = &watcher{
		cordCtx:      cordCtx,
		path:         ec.taskPath,
		responseChan: make(chan *etcd.Response),
		errorChan:    make(chan error),
		client:       ec.Client,
	}

	ec.commandWatcher = &watcher{
		cordCtx:      cordCtx,
		path:         ec.commandPath,
		responseChan: make(chan *etcd.Response),
		errorChan:    make(chan error),
		client:       ec.Client,
	}

	ec.taskManager = newManager(cordCtx, ec.Client, ec.taskPath, ec.NodeID, ec.ClaimTTL)
	return nil
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

func (ec *EtcdCoordinator) nodeRefresher() {
	ttl := ec.nodePathTTL - 3 // try to have 3s of leeway before ttl expires
	if ttl < 1 {
		ttl = 1
	}
	for {
		select {
		case <-ec.stopNode:
			return
		case <-time.After(time.Duration(ttl) * time.Second):
			if _, err := ec.Client.UpdateDir(ec.nodePath, ec.nodePathTTL); err != nil {
				ec.cordCtx.Log(metafora.LogLevelError, "Unexpected error updating node key, shutting down. Error: %v", err)

				// We're in a bad state; shut everything down
				ec.Close()
				return
			}

		}
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
				ec.cordCtx.Log(metafora.LogLevelDebug, "New task while watching %s: action=%v etcdIndex=%v nodeKey=%v]",
					ec.taskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)
				if taskId, ok := ec.parseTaskIdFromTaskNode(resp.Node); ok {
					return taskId, nil
				}
			}

			//The etcd watcher may have received a released task or an owner may have left the cluster.
			//Ref: etcd event types : https://github.com/coreos/etcd/blob/master/store/event.go#L4
			if (resp.Action == "expire" || resp.Action == "delete" || resp.Action == "compareAndDelete") &&
				ec.nodeIsTheOwnerMarker(resp.Node) {

				ec.cordCtx.Log(metafora.LogLevelDebug, "Released task signaled while watching %s: response[action:%v etcdIndex:%v nodeKey:%v]",
					ec.taskPath, resp.Action, resp.EtcdIndex, resp.Node.Key)

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
	res := strings.Replace(nodePath, ec.taskPath+"/", "", 1)
	//remove OwnerMarker
	res2 := strings.Replace(res, "/"+OwnerMarker, "", 1)
	//if remainder doens't contain "/", then we've found the taskid
	if !strings.Contains(res2, "/") {
		return res2, false
	} else {
		return "", true
	}
}

func (ec *EtcdCoordinator) parseTaskIdFromTaskNode(node *etcd.Node) (taskID string, ok bool) {
	if !strings.HasPrefix(node.Key, ec.taskPath) {
		return
	}

	if ec.nodeHasOwnerMarker(node) {
		return
	}

	if !node.Dir {
		return
	}

	taskID = path.Base(node.Key)

	if taskID == "tasks" {
		return
	}

	ec.cordCtx.Log(metafora.LogLevelDebug, "A claimable task was found. task:%s key:%s", taskID, node.Key)
	return taskID, true
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
	// Gracefully handle multiple close calls mostly to ease testing
	ec.closeL.Lock()
	defer ec.closeL.Unlock()
	if ec.closed {
		return
	}
	ec.closed = true

	ec.taskWatcher.stop()
	ec.commandWatcher.stop()
	ec.taskManager.stop()

	// Finally remove the node entry
	close(ec.stopNode)

	const recursive = true
	_, err := ec.Client.Delete(ec.nodePath, recursive)
	if err != nil {
		if eerr, ok := err.(*etcd.EtcdError); ok {
			if eerr.ErrorCode == EcodeKeyNotFound {
				// The node's TTL was up before we were able to delete it or there was
				// another problem that's already being handled.
				// The first is unlikely, the latter is already being handled, so
				// there's nothing to do here.
				return
			}
		}
		// All other errors are unexpected
		ec.cordCtx.Log(metafora.LogLevelError, "Error deleting node path %s: %v", ec.nodePath, err)
	}
}
