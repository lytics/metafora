package m_etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type nHistory struct {
	Path string
	Time time.Time
}

func TestTtlRefresherExecutionOrder(t *testing.T) {
	runHistory := []*nHistory{}
	nRefresher := &NodeRefresher{
		addTaskChannel:    make(chan *RefreshableEtcdDir),
		removeTaskChannel: make(chan string),
		refreshtasks:      make(map[string]*RefreshableEtcdDir),
		ttlHeap:           newRefreshableDirHeap(),
		pathToNodeMap:     make(map[string]*RefreshableEtcdDir),
		RefreshFunction: func(node_path string, ttl int64) error {
			runHistory = append(runHistory, &nHistory{Path: node_path, Time: time.Now()})
			return nil
		},
	}

	nRefresher.StartScheduler()
	defer nRefresher.Close()
	testPaths := []string{"1", "2", "3"}
	nRefresher.ScheduleRefresh(testPaths[0], 1)
	nRefresher.ScheduleRefresh(testPaths[1], 2)
	nRefresher.ScheduleRefresh(testPaths[2], 3)

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
		addTaskChannel:    make(chan *RefreshableEtcdDir),
		removeTaskChannel: make(chan string),
		refreshtasks:      make(map[string]*RefreshableEtcdDir),
		ttlHeap:           newRefreshableDirHeap(),
		pathToNodeMap:     make(map[string]*RefreshableEtcdDir),
		RefreshFunction: func(node_path string, ttl int64) error {
			runHistory = append(runHistory, &nHistory{Path: node_path, Time: time.Now()})
			return nil
		},
	}

	nRefresher.StartScheduler()
	defer nRefresher.Close()
	testPaths := []string{"2"}
	nRefresher.ScheduleRefresh(testPaths[0], 2)
	time.Sleep(1 * time.Second)
	if len(runHistory) > 0 {
		t.Fatal("No tasks should have fired yet, as the timer is for 2 seconds and its only been one second.")
	}
	time.Sleep(1 * time.Second)
	if len(runHistory) != 1 {
		t.Fatal("The tasks should have fired once and only once.  But it fired %d times.", len(runHistory))
	}
	nRefresher.UnscheduleRefresh(testPaths[0])
	time.Sleep(2 * time.Second)
	if len(runHistory) != 1 {
		t.Fatal("The tasks shouldn't have fired again.  But it most have because fired times is now %d.", len(runHistory))
	}
}

func TestTtlRefresherIntegration(t *testing.T) {
	eclient := newEtcdClient(t)
	cleanupNameSpace(t, TestNameSpace)
	refresher := NewNodeRefresher(eclient, testLogger{"NodeRefresher", t})
	refresher.StartScheduler()

	//TestCase 1
	//
	t.Log("TestCase 1: add a etcd dir, then a start the refresher for it.  Wait past it's TTL and make sure it's still around.\n")

	const testDir = TestNameSpace + "/foobar"
	const testNode = TestNameSpace + "/foobar/test.txt"
	const testVal = "Hello Metafora"

	eclient.CreateDir(testDir, 1)
	eclient.Create(testNode, testVal, 0)
	const sort = false
	const recursive = false
	resp, err := eclient.Get(testNode, sort, recursive)
	if err != nil {
		t.Fatalf("Error trying to get back our test node.  Error %s", err.Error())
	}

	refresher.ScheduleRefresh(testDir, 1)

	time.Sleep(3 * time.Second)

	//First make sure the etcd key is still around.
	resp, err = eclient.Get(testNode, sort, recursive)
	if err != nil {
		t.Fatalf("Error trying to get back our test node.  Error %s", err.Error())
	}
	if resp.Node.Key != testNode || resp.Node.Value != "Hello Metafora" || resp.Node.TTL != 0 {
		t.Fatalf("The etcd key was successfully retrieved but its data was changed."+
			"\ngot       key[%s], value[%s], ttl[%d]"+
			"\nexpected  key[%s], value[%s], ttl[%d]",
			resp.Node.Key, resp.Node.Value, resp.Node.TTL, testNode, testVal, 0)
	}

	//Then make sure the dir is still around.
	resp, err = eclient.Get(testDir, sort, recursive)
	if err != nil {
		t.Fatalf("Error trying to get back our test dir back.  Error %s", err.Error())
	}
	if resp.Node.Key != testDir || resp.Node.TTL != 1 {
		t.Fatalf("The etcd key was successfully retrieved but its data was changed."+
			"\ngot       key[%s], ttl[%d]"+
			"\nexpected  key[%s], ttl[%d]",
			resp.Node.Key, resp.Node.TTL, testDir, 1)
	}

	//TestCase 2
	//
	t.Log("TestCase2: remove the path from the refresher and make sure the etcd dir expires after the TTL\n")

	refresher.UnscheduleRefresh(testDir)
	time.Sleep(1 * time.Second)

	//First make sure the etcd key is still around.
	resp, err = eclient.Get(testNode, sort, recursive)
	if err == nil {
		t.Fatal("Error: was expecting a EcodeKeyNotFound error from etcd, but got no error back...")
	} else {
		etcdErr, ok := err.(*etcd.EtcdError)
		if !ok || etcdErr.ErrorCode != EcodeKeyNotFound {
			t.Fatalf("Error: was expecting a EcodeKeyNotFound error from etcd but got some other error back. Error %s", err.Error())
		}
	}

	resp, err = eclient.Get(testDir, sort, recursive)
	if err == nil {
		t.Fatal("Error: was expecting a EcodeKeyNotFound error from etcd, but got no error back...")
	} else {
		etcdErr, ok := err.(*etcd.EtcdError)
		if !ok || etcdErr.ErrorCode != EcodeKeyNotFound {
			t.Fatalf("Error: was expecting a EcodeKeyNotFound error from etcd but got some other error back. Error %s", err.Error())
		}
	}

	refresher.Close()
}
