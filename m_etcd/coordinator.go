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

const (
	actionCreated = "create"
	actionExpire  = "expire"
	actionDelete  = "delete"
	actionCAD     = "compareAndDelete"
)

var (
	DefaultNodePathTTL uint64 = 20 // seconds

	// etcd actions signifying a claim key was released
	releaseActions = map[string]bool{
		actionExpire: true,
		actionDelete: true,
		actionCAD:    true,
	}
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

	index := resp.Node.ModifiedIndex

	// Act like these are newly created nodes
	for _, node := range resp.Node.Nodes {
		if node.ModifiedIndex > index {
			// Record the max modified index to keep Watch from picking up redundant events
			index = node.ModifiedIndex
		}
		select {
		case <-w.stopChan:
			return
		case w.responseChan <- &etcd.Response{Action: actionCreated, Node: node, EtcdIndex: resp.EtcdIndex}:
		}
	}

	var rawResp *etcd.RawResponse

	// Start blocking watch
	for {
		// Start the blocking watch after the last response's index.
		rawResp, err = w.client.RawWatch(w.path, index+1, recursive, nil, w.stopChan)
		if err != nil {
			if err == etcd.ErrWatchStoppedByUser {
				// This isn't actually an error, the stopChan was closed. Time to stop!
				err = nil
			} else {
				// RawWatch errors should be treated as fatal since it internally
				// retries on network problems, and recoverable Etcd errors aren't
				// parsed until rawResp.Unmarshal is called later.
				w.cordCtx.Log(metafora.LogLevelError, "Unrecoverable watch error: %v", err)
			}
			return
		}

		if len(rawResp.Body) == 0 {
			// This is a bug in Go's HTTP + go-etcd + etcd which causes the
			// connection to timeout perdiocally and need to be restarted *after*
			// closing idle connections.
			w.cordCtx.Log(metafora.LogLevelDebug, "Watch response empty; restarting watch")
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

		index = resp.Node.ModifiedIndex
	}
}

// ensureWatching starts watching etcd in a goroutine if it's not already running.
func (w *watcher) ensureWatching() {
	w.m.Lock()
	defer w.m.Unlock()
	if !w.running {
		// Initialize watch state here; will be updated by watch()
		w.running = true
		w.stopChan = make(chan bool)
		go w.watch()
	}
}

// stops the watching goroutine.
func (w *watcher) stop() {
	w.m.Lock()
	defer w.m.Unlock()
	if w.running {
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
func (ec *EtcdCoordinator) Watch() (taskID string, err error) {
	ec.taskWatcher.ensureWatching()

watchLoop:
	for {
		select {
		case resp, ok := <-ec.taskWatcher.responseChan:
			if !ok {
				return "", nil
			}

			// Sanity check / test path invariant
			if !strings.HasPrefix(resp.Node.Key, ec.taskPath) {
				ec.cordCtx.Log(metafora.LogLevelError, "Received task from outside task path: %s", resp.Node.Key)
				continue
			}

			key := strings.Trim(resp.Node.Key, "/") // strip leading and trailing /s
			parts := strings.Split(key, "/")

			// Pickup new tasks
			if resp.Action == actionCreated && len(parts) == 3 && resp.Node.Dir {
				// Make sure it's not already claimed before returning it
				for _, n := range resp.Node.Nodes {
					if strings.HasSuffix(n.Key, OwnerMarker) {
						ec.cordCtx.Log(metafora.LogLevelDebug, "Ignoring task as it's already claimed: %s", parts[2])
						continue watchLoop
					}
				}
				ec.cordCtx.Log(metafora.LogLevelDebug, "Received new task: %s", parts[2])
				return parts[2], nil
			}

			// If a claim key is removed, try to claim the task
			if releaseActions[resp.Action] && len(parts) == 4 && parts[3] == OwnerMarker {
				ec.cordCtx.Log(metafora.LogLevelDebug, "Received released task: %s", parts[2])
				return parts[2], nil
			}

			// Ignore everything else
			ec.cordCtx.Log(metafora.LogLevelDebug, "Ignoring key in tasks: %s", resp.Node.Key)
		case err := <-ec.taskWatcher.errorChan:
			return "", err
		}
	}
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
	ec.commandWatcher.ensureWatching()

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
