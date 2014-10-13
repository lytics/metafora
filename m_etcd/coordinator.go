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

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	Namespace string
	TaskPath  string

	taskWatcher       *EtcdWatcher
	tasksPrefetchChan chan *etcd.Node
	ClaimTTL          uint64

	NodeID         string
	CommandPath    string
	commandWatcher *EtcdWatcher

	refresher *NodeRefresher

	IsClosed bool
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
		ClaimTTL:    ClaimTTL, //default to the package constant, but allow it to be overwritten

		NodeID:         nodeId,
		CommandPath:    fmt.Sprintf("/%s/%s/%s/%s", namespace, NodesPath, nodeId, CommandsPath),
		commandWatcher: nil,

		refresher: nil,

		IsClosed: false,
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

	// starts the run loop that processes ttl refreshes in a background go routine.
	ec.refresher = NewNodeRefresher(ec.Client, cordCtx)
	ec.refresher.StartScheduler()
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

			taskId, skip := ec.parseTaskIdFromTaskNode(node)
			if !skip {
				return taskId, nil
			}
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

		case err, ok := <-ec.taskWatcher.errorChan:
			if !ok {
				return "", nil
			}
			//if the watcher sends a nil, its because the etcd watcher was shutdown by Close()
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
	claimedMarker := fmt.Sprintf("%s/%s/%s", ec.TaskPath, taskID, OwnerMarker)
	res, err := ec.Client.CreateDir(claimedMarker, ec.ClaimTTL)
	if err != nil {
		etcdErr, ok := err.(*etcd.EtcdError)
		if !ok || etcdErr.ErrorCode != EcodeNodeExist {
			ec.cordCtx.Log(metafora.LogLevelError, "Claim failed with an expected error: key%s err %v", claimedMarker, err)
		} else {
			ec.cordCtx.Log(metafora.LogLevelDebug, "Claim failed, it appears someone else got it first: resp %v", res)
		}
		return false
	}
	ec.cordCtx.Log(metafora.LogLevelDebug, "Claim successful: resp %v", res)

	//add a scheduled tasks to refresh the ./owner/ dir's ttl until the coordinator is shutdown.
	ec.refresher.ScheduleDirRefresh(claimedMarker, ec.ClaimTTL)

	return true
}

// Release deletes the claim directory.
func (ec *EtcdCoordinator) Release(taskID string) {
	claimedMarker := fmt.Sprintf("%s/%s/%s", ec.TaskPath, taskID, OwnerMarker)
	//FIXME Conditionally delete only if this node is actually the owner
	ec.refresher.UnscheduleDirRefresh(claimedMarker)
	_, err := ec.Client.DeleteDir(claimedMarker)
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
	if !ec.IsClosed {
		ec.taskWatcher.Close()
		ec.commandWatcher.Close()
		ec.refresher.Close()
		ec.IsClosed = true
	}
	return nil
}
