package m_etcd

import (
	"container/heap"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type TTLRefreshNode struct {
	Node        string
	TTLInterval int64
	NextRun     time.Time
}

// The Scheduler runs reoccurring tasks on an interval for this coordinator.
// For example updating the TTLs for etcd paths we've claimed.
type NodeRefresher struct {
	addTaskChannel    chan *TTLRefreshNode
	removeTaskChannel chan string
	refreshtasks      map[string]*TTLRefreshNode
	ttlHeap           *ttlRefreshNodeHeap
	pathToNodeMap     map[string]*TTLRefreshNode
	RefreshFunction   func(key string, ttl int64) error
}

func NewNodeRefresher(client *etcd.Client) *NodeRefresher {
	return &NodeRefresher{
		addTaskChannel:    make(chan *TTLRefreshNode),
		removeTaskChannel: make(chan string),
		refreshtasks:      make(map[string]*TTLRefreshNode),
		ttlHeap:           newTTLRefreshNodeHeap(),
		pathToNodeMap:     make(map[string]*TTLRefreshNode),
		RefreshFunction:   NewDefaultRefreshFunction(client),
	}
}

func NewDefaultRefreshFunction(client *etcd.Client) func(string, int64) error {
	return func(node_path string, ttl int64) error {
		_, err := client.RawUpdate(node_path, "", uint64(ttl)) //an empty value only updates the ttl
		return err
	}
}

func (s *NodeRefresher) ScheduleTTLRefresh(node_path string, ttl int64) {
	node := &TTLRefreshNode{
		Node:        node_path,
		TTLInterval: ttl,
	}
	s.updateNextRun(node)
	s.addTaskChannel <- node // signal the run loop that we have a new node_path to update
}

func (s *NodeRefresher) UnscheduleTTLRefresh(node_path string) {
	s.removeTaskChannel <- node_path // signal the run loop to stop updating the ttl for the node_path
}

func (s *NodeRefresher) StartScheduler() {
	go func() {
		//This runs in the background were it does the following:
		//  1) listens on the inbound channel for new etcd paths
		//  2) on an interval it updates the TTL for all paths

		for {
			select {
			case refreshTask, ok := <-s.addTaskChannel:
				if !ok {
					return
				}
				s.scheduleNode(refreshTask)
			case path, ok := <-s.removeTaskChannel:
				if !ok {
					return
				}
				s.unschedulePath(path)
			case <-time.After(time.Millisecond * 250):
				// Using a frequency of 4 times a second, so that if we have a ttl of 1 second, we can update
				// it every 3/4 of a second.  Note that in updateNextRun() we use the ttl - 300ms as the
				// refresh interval.  Because we want to update the ttl before it times out...
				nodes := s.allExcutableNodes(time.Now())
				for _, node := range nodes {
					s.RefreshFunction(node.Node, node.TTLInterval) //do the refresh
					s.updateNextRun(node)                          //figure out when the next schedule is
					s.scheduleNode(node)                           //schedule this node to be updated then
				}
			}
		}
	}()
}

//Internal functions to support the scheduler
//
func (s *NodeRefresher) updateNextRun(node *TTLRefreshNode) {
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

//Return the node with the next available time to run
func (s *NodeRefresher) allExcutableNodes(now time.Time) []*TTLRefreshNode {
	results := []*TTLRefreshNode{}
	for MinTime(s.ttlHeap).Before(now) {
		node := s.nextExcutableNode()
		results = append(results, node)
	}
	return results
}

func (s *NodeRefresher) nextExcutableNode() *TTLRefreshNode {
	x := heap.Pop(s.ttlHeap)
	node, _ := x.(*TTLRefreshNode)
	delete(s.pathToNodeMap, node.Node)
	return node
}

func (s *NodeRefresher) scheduleNode(node *TTLRefreshNode) {
	s.pathToNodeMap[node.Node] = node
	heap.Push(s.ttlHeap, node)
}

func (s *NodeRefresher) unschedulePath(p string) {
	if node, ok := s.pathToNodeMap[p]; ok {
		s.unscheduleNode(node)
	}
}

func (s *NodeRefresher) unscheduleNode(node *TTLRefreshNode) {
	index, ok := s.ttlHeap.nodeToIndexMap[node]
	if ok {
		delete(s.pathToNodeMap, node.Node)
		heap.Remove(s.ttlHeap, index)
	}
}

func MinTime(h *ttlRefreshNodeHeap) time.Time {
	if h.Len() != 0 {
		return h.bkArr[0].NextRun
	}
	return time.Now().Add(time.Hour * 10000) //just some time in the future
}

/////////////////////////////////////////////////////////////////////
// Internal Min Heap stuff so ttlRefreshNodeHeap satisfies the Heap interface
//
//  DON'T CALL THESE DIRECTLY
//
// An TTLRefreshNodeHeap is a min-heap of TTLKeys order by expiration time
//
type ttlRefreshNodeHeap struct {
	bkArr          []*TTLRefreshNode
	nodeToIndexMap map[*TTLRefreshNode]int
}

func newTTLRefreshNodeHeap() *ttlRefreshNodeHeap {
	h := &ttlRefreshNodeHeap{nodeToIndexMap: make(map[*TTLRefreshNode]int)}
	heap.Init(h)
	return h
}

func (h ttlRefreshNodeHeap) Len() int {
	return len(h.bkArr)
}

func (h ttlRefreshNodeHeap) Less(i, j int) bool {
	return h.bkArr[i].NextRun.Before(h.bkArr[j].NextRun)
}

func (h ttlRefreshNodeHeap) Swap(i, j int) {
	// swap
	h.bkArr[i], h.bkArr[j] = h.bkArr[j], h.bkArr[i]

	// Update the index map to reflex the swap
	h.nodeToIndexMap[h.bkArr[i]] = i
	h.nodeToIndexMap[h.bkArr[j]] = j
}

func (h *ttlRefreshNodeHeap) Push(x interface{}) {
	n, _ := x.(*TTLRefreshNode)
	h.nodeToIndexMap[n] = len(h.bkArr)
	h.bkArr = append(h.bkArr, n)
}

func (h *ttlRefreshNodeHeap) Pop() interface{} {
	old := h.bkArr
	n := len(old)
	x := old[n-1]
	h.bkArr = old[0 : n-1]
	delete(h.nodeToIndexMap, x)
	return x
}
