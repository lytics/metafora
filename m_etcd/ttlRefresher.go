package m_etcd

import (
	"container/heap"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type RefreshableEtcdDir struct {
	Node        string
	TTLInterval int64
	NextRun     time.Time
}

// The Scheduler runs reoccurring tasks on an interval for this coordinator.
// For example updating the TTLs for etcd paths we've claimed.
type NodeRefresher struct {
	addTaskChannel    chan *RefreshableEtcdDir
	removeTaskChannel chan string
	stopChannel       chan bool
	refreshtasks      map[string]*RefreshableEtcdDir
	ttlHeap           *RefreshableEtcdDirHeap
	pathToNodeMap     map[string]*RefreshableEtcdDir
	RefreshFunction   func(key string, ttl int64) error
	isCloseable       bool
	cordCtx           metafora.CoordinatorContext
}

func NewNodeRefresher(client *etcd.Client, cordCtx metafora.CoordinatorContext) *NodeRefresher {
	return &NodeRefresher{
		addTaskChannel:    make(chan *RefreshableEtcdDir),
		removeTaskChannel: make(chan string),
		refreshtasks:      make(map[string]*RefreshableEtcdDir),
		ttlHeap:           newRefreshableDirHeap(),
		pathToNodeMap:     make(map[string]*RefreshableEtcdDir),
		RefreshFunction:   NewDefaultRefreshFunction(client, cordCtx),
		isCloseable:       false,
		cordCtx:           cordCtx,
	}
}

//Returns a function used to update the ttl in etcd.  For testing reasons, you can create your own
// RefreshFunction.  That way we can test the Refresher without etcd.
func NewDefaultRefreshFunction(client *etcd.Client, cordCtx metafora.CoordinatorContext) func(string, int64) error {
	return func(node_path string, ttl int64) error {
		_, err := client.RawUpdate(node_path, "", uint64(ttl)) //an empty value only updates the ttl
		if err != nil {
			cordCtx.Log(metafora.LogLevelError, "Error trying to update node[%s]'s ttl.  Error:%s", node_path, err.Error())
		}
		return err
	}
}

// This schedules a ttl refresh of a etcd directory.  It you try to refresh a etcd key,
// the default RefreshFunction will not function correctly as it will unset the key's
// value.  The common pattern in etcd is to refresh the directory ttl of the directory
// containing the key.
// Ref:
// https://github.com/coreos/etcd/issues/385
// https://github.com/coreos/etcd/issues/1232
func (s *NodeRefresher) ScheduleDirRefresh(etcd_dir string, ttl uint64) {
	node := &RefreshableEtcdDir{
		Node:        etcd_dir,
		TTLInterval: int64(ttl),
	}
	s.updateNextRun(node)
	s.addTaskChannel <- node // signal the run loop that we have a new node_path to update
}

func (s *NodeRefresher) UnscheduleDirRefresh(etcd_dir string) {
	s.removeTaskChannel <- etcd_dir // signal the run loop to stop updating the ttl for the node_path
}

//This starts the main run loop for the scheduler.  The run loops monitors for new/removed etcd
//node_removal schedules and on an interval it executes the overdue node-updates.
//
// Node if you call StartScheduler() after calling Close() your very likely to get a panic.
func (s *NodeRefresher) StartScheduler() {
	s.stopChannel = make(chan bool)
	s.isCloseable = true
	go func() {
		for {
			select {
			//listen on the channel for new etcd paths
			case refreshTask, ok := <-s.addTaskChannel:
				if !ok {
					return
				}
				s.scheduleNode(refreshTask)
			//listen on the channel for removed etcd paths
			case path, ok := <-s.removeTaskChannel:
				if !ok {
					return
				}
				s.unschedulePath(path)
			//Every 250 milliseconds look for Nodes that have a next-run-time before time.Now(), using the min heap below to be efficient.
			case <-time.After(time.Millisecond * 250):
				// Using a frequency of 4 times a second, so that if we have a ttl of 1 second, we can update
				// it every 3/4 of a second.  Note that in updateNextRun() we use the ttl - 300ms as the
				// refresh interval.  Because we want to update the ttl before it times out...
				nodes := s.allExcutableNodes(time.Now())
				for _, node := range nodes {
					s.RefreshFunction(node.Node, node.TTLInterval) //do the refresh and update the node's ttl
					s.updateNextRun(node)                          //calculate the nodes next scheduled ttl update
					s.scheduleNode(node)                           //schedule this node to be updated then
				}
			case <-s.stopChannel:
				return
			}
		}
	}()
}

func (s *NodeRefresher) Close() {
	if s.isCloseable {
		close(s.addTaskChannel)
		close(s.removeTaskChannel)
		close(s.stopChannel)
		s.isCloseable = false
	}
}

//Internal functions to support the scheduler
//
func (s *NodeRefresher) updateNextRun(node *RefreshableEtcdDir) {
	//There isn't any science here.
	// Basically we want to update the TTL before it expires
	//
	ttlDur := time.Duration(node.TTLInterval)
	if node.TTLInterval <= 5 {
		node.NextRun = time.Now().Add(ttlDur * time.Second).Add(time.Millisecond * -300)
	} else if node.TTLInterval <= 15 {
		node.NextRun = time.Now().Add((ttlDur - 1) * time.Second).Add(time.Millisecond * -300)
	} else {
		node.NextRun = time.Now().Add((ttlDur - 2) * time.Second).Add(time.Millisecond * -300)
	}
}

//Return all the nodes with an available runtime before the parameter time limit.
func (s *NodeRefresher) allExcutableNodes(limit time.Time) []*RefreshableEtcdDir {
	results := []*RefreshableEtcdDir{}
	for minTime(s.ttlHeap).Before(limit) {
		node := s.nextExcutableNode()
		results = append(results, node)
	}
	return results
}

//get the node-struct from the min-heap with the next run time.
func (s *NodeRefresher) nextExcutableNode() *RefreshableEtcdDir {
	x := heap.Pop(s.ttlHeap)
	node, _ := x.(*RefreshableEtcdDir)
	delete(s.pathToNodeMap, node.Node)
	return node
}

//adds a node-struct to the min-heap and our path to node-struct map
func (s *NodeRefresher) scheduleNode(node *RefreshableEtcdDir) {
	s.pathToNodeMap[node.Node] = node
	heap.Push(s.ttlHeap, node)
}

//used to remove a node by path from the min-heap.
// looks up the node-struct in a map and calls unscheduleNode(node *RefreshableEtcdDir)
func (s *NodeRefresher) unschedulePath(p string) {
	if node, ok := s.pathToNodeMap[p]; ok {
		s.unscheduleNode(node)
	}
}

//used to remove a node by node-pointer from the min heap, and our path to node-struct map
func (s *NodeRefresher) unscheduleNode(node *RefreshableEtcdDir) {
	index, ok := s.ttlHeap.nodeToIndexMap[node]
	if ok {
		delete(s.pathToNodeMap, node.Node)
		heap.Remove(s.ttlHeap, index)
	}
}

//Basically this func allows you to pick at the timestamp for the min time in the heap.
func minTime(h *RefreshableEtcdDirHeap) time.Time {
	if h.Len() != 0 {
		return h.bkArr[0].NextRun
	}
	return time.Now().Add(time.Hour * 10000) //just some time in the future
}

/////////////////////////////////////////////////////////////////////
// Internal Min Heap stuff so RefreshableEtcdDirHeap satisfies the Heap interface
//
//  DON'T CALL THESE DIRECTLY
//
// An RefreshableEtcdDirHeap is a min-heap of TTLKeys order by expiration time
//
type RefreshableEtcdDirHeap struct {
	bkArr          []*RefreshableEtcdDir
	nodeToIndexMap map[*RefreshableEtcdDir]int
}

func newRefreshableDirHeap() *RefreshableEtcdDirHeap {
	h := &RefreshableEtcdDirHeap{nodeToIndexMap: make(map[*RefreshableEtcdDir]int)}
	heap.Init(h)
	return h
}

func (h RefreshableEtcdDirHeap) Len() int {
	return len(h.bkArr)
}

func (h RefreshableEtcdDirHeap) Less(i, j int) bool {
	return h.bkArr[i].NextRun.Before(h.bkArr[j].NextRun)
}

func (h RefreshableEtcdDirHeap) Swap(i, j int) {
	// swap
	h.bkArr[i], h.bkArr[j] = h.bkArr[j], h.bkArr[i]

	// Update the index map to reflex the swap
	h.nodeToIndexMap[h.bkArr[i]] = i
	h.nodeToIndexMap[h.bkArr[j]] = j
}

func (h *RefreshableEtcdDirHeap) Push(x interface{}) {
	n, _ := x.(*RefreshableEtcdDir)
	h.nodeToIndexMap[n] = len(h.bkArr)
	h.bkArr = append(h.bkArr, n)
}

func (h *RefreshableEtcdDirHeap) Pop() interface{} {
	old := h.bkArr
	n := len(old)
	x := old[n-1]
	h.bkArr = old[0 : n-1]
	delete(h.nodeToIndexMap, x)
	return x
}
