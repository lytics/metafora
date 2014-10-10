package m_etcd

import (
	"fmt"
	"testing"
	"time"
)

type nHistory struct {
	Path string
	Time time.Time
}

func TestTtlRefresherExecutionOrder(t *testing.T) {
	runHistory := []*nHistory{}
	nRefresher := &NodeRefresher{
		addTaskChannel:    make(chan *TTLRefreshNode),
		removeTaskChannel: make(chan string),
		refreshtasks:      make(map[string]*TTLRefreshNode),
		ttlHeap:           newTTLRefreshNodeHeap(),
		pathToNodeMap:     make(map[string]*TTLRefreshNode),
		RefreshFunction: func(node_path string, ttl int64) error {
			runHistory = append(runHistory, &nHistory{Path: node_path, Time: time.Now()})
			return nil
		},
	}

	nRefresher.StartScheduler()
	testPaths := []string{"1", "2", "3"}
	nRefresher.ScheduleTTLRefresh(testPaths[0], 1)
	nRefresher.ScheduleTTLRefresh(testPaths[1], 2)
	nRefresher.ScheduleTTLRefresh(testPaths[2], 3)

	time.Sleep(4 * time.Second)

	exeOrder := ""
	for _, his := range runHistory {
		exeOrder += fmt.Sprintf("%s", his.Path)
	}

	if exeOrder != "11213121" {
		//1 should trigger every 750ms,   floor(4000/750)  == 5 times
		//2 should trigger every 1750ms,  floor(4000/1750) == 2 times
		//3 should trigger every 2750ms,  floor(4000/2750) == 1 time
		t.Fatalf("The paths didn't update in the order I'd expect.  The order was:\n%s", exeOrder)
	}
}

func TestTtlRefresherTiming(t *testing.T) {
	runHistory := []*nHistory{}
	nRefresher := &NodeRefresher{
		addTaskChannel:    make(chan *TTLRefreshNode),
		removeTaskChannel: make(chan string),
		refreshtasks:      make(map[string]*TTLRefreshNode),
		ttlHeap:           newTTLRefreshNodeHeap(),
		pathToNodeMap:     make(map[string]*TTLRefreshNode),
		RefreshFunction: func(node_path string, ttl int64) error {
			runHistory = append(runHistory, &nHistory{Path: node_path, Time: time.Now()})
			return nil
		},
	}

	nRefresher.StartScheduler()
	testPaths := []string{"2"}
	nRefresher.ScheduleTTLRefresh(testPaths[0], 2)
	time.Sleep(1 * time.Second)
	if len(runHistory) > 0 {
		t.Fatal("No tasks should have fired yet, as the timer is for 2 seconds and its only been one second.")
	}
	time.Sleep(1 * time.Second)
	if len(runHistory) != 1 {
		t.Fatal("The tasks should have fired once and only once.  But it fired %s times.", len(runHistory))
	}
	nRefresher.UnscheduleTTLRefresh(testPaths[0])
	time.Sleep(2 * time.Second)
	if len(runHistory) != 1 {
		t.Fatal("The tasks should have fired once and only once.  But it fired %s times.", len(runHistory))
	}
}
