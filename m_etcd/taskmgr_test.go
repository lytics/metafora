package m_etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type fakeEtcd struct {
	add chan string
	del chan string
	cas chan string
	cad chan string
}

func (f fakeEtcd) Create(key, value string, ttl uint64) (*etcd.Response, error) {
	f.add <- key
	return nil, nil
}

func (f fakeEtcd) Delete(key string, recursive bool) (*etcd.Response, error) {
	f.del <- key
	return nil, nil
}

func (f fakeEtcd) CompareAndDelete(k, pv string, _ uint64) (*etcd.Response, error) {
	f.cad <- k
	return nil, nil
}

func (f fakeEtcd) CompareAndSwap(k, v string, ttl uint64, pv string, _ uint64) (*etcd.Response, error) {
	if k == "testns/testlost/owner" {
		return nil, fmt.Errorf("test error")
	}
	f.cas <- k
	return nil, nil
}
func newFakeEtcd() fakeEtcd {
	return fakeEtcd{
		add: make(chan string, 10),
		del: make(chan string, 10),
		cas: make(chan string, 10),
		cad: make(chan string, 10),
	}
}

// Test that tasks are refreshed periodically.
func TestTaskRefreshing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	client := newFakeEtcd()
	const ttl = 2
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)
	mgr.add("tid")
	for i := 0; i < 2; i++ {
		select {
		case <-client.cas:
			t.Log("Task refreshed.")
		case <-client.cad:
			t.Errorf("Task deleted?! This isn't right at all.")
		case <-time.After(4 * time.Second):
			t.Errorf("Task wasn't refreshed soon enough.")
		}
	}
}

// Test that tasks can be removed before they're even refreshed.
func TestTaskRemoval(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	client := newFakeEtcd()
	const ttl = 2
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)
	mgr.add("tid")
	mgr.remove("tid", false)
	select {
	case <-client.add:
		// Yay, everything worked
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Task wasn't added soon enough.")
	}
	select {
	case <-client.cad:
		// Yay, everything worked
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Task wasn't removed soon enough.")
	}

	select {
	case <-client.cas:
		t.Errorf("Task shouldn't have lived long enough to be CAS'd")
	default:
	}
}

// Test multiple tasks can be added, will be refreshed, removed, and stopped.
func TestFullTaskMgr(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	client := newFakeEtcd()
	const ttl = 2
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)

	// Add a few tasks and remove one
	mgr.add("tid1")
	mgr.add("tid2")
	mgr.add("tid3")
	mgr.remove("tid2", false)

	delDone := false
	expectedSwaps := map[string]bool{mgr.ownerKey("tid1"): true, mgr.ownerKey("tid3"): true}

	// Test that only expected actions occurred (and in a timely manner)
	for i := 0; i < 3; i++ {
		select {
		case path := <-client.cas:
			if !expectedSwaps[path] {
				t.Errorf("CAS'd unexpected task: %s", path)
			}
			delete(expectedSwaps, path)
		case path := <-client.cad:
			if path != mgr.ownerKey("tid2") {
				t.Errorf("Deleted unexpected task: %s", path)
			}
			if delDone {
				t.Errorf("Deleted tid2 twice!")
			}
			delDone = true
		case <-time.After(1500 * time.Millisecond):
			t.Fatalf("Took too long for refreshes to happen")
		}
	}

	// Calling remove concurrently should be safe
	go mgr.remove("tid3", false)
	go mgr.remove("tid1", false)
	expectedDels := map[string]bool{mgr.ownerKey("tid1"): true, mgr.ownerKey("tid3"): true}
	for len(expectedDels) > 0 {
		select {
		case path := <-client.cad:
			if !expectedDels[path] {
				t.Errorf("Removed unexpected task: %s", path)
			}
			delete(expectedDels, path)
		case <-time.After(1 * time.Second):
			t.Fatalf("Took too long for deletes to happen")
		}
	}

	if len(client.cad) > 0 {
		t.Errorf("Unexpected deletes occurred")
	}
}

// Test that losing tasks who cannot be refreshed works.
func TestTaskLost(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	ctx := newCtx(t, "mgr")
	client := newFakeEtcd()
	const ttl = 2
	mgr := newManager(ctx, client, "testns", "testnode", ttl)

	mgr.add("testlost")

	// Wait for the CAS to fail
	time.Sleep(ttl * time.Second)

	if len(client.cas) > 0 {
		t.Error("Unexpected CAS. Should have failed.")
	}
	n := len(ctx.lost)
	if n != 1 {
		t.Fatalf("Expected 1 lost task, but found %d.", n)
	}
	if <-ctx.lost != "testlost" {
		t.Fatalf("Lost a different task ID than expected! Oh my.")
	}

	// removing a lost task should be a noop
	mgr.remove("testlost", false)

	if len(client.cad) > 0 {
		t.Error("Unexpectedly deleted non-existant tasks when shutting down.")
	}
}

// Test that marking tests as done calls delete.
func TestTaskDone(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	ctx := newCtx(t, "mgr")
	client := newFakeEtcd()
	const ttl = 2
	mgr := newManager(ctx, client, "testns", "testnode", ttl)

	mgr.add("t1")
	mgr.add("t2")
	mgr.remove("t1", true)
	mgr.remove("t2", false)

	// Should have 1 CAD and 1 Delete
	<-client.cad
	<-client.del

	if len(client.cas) > 0 {
		t.Errorf("Expected 0 CASs but found %d", len(client.cas))
	}
	if len(client.cad) > 0 {
		t.Errorf("Expected 1 CAD but found %d", len(client.cad)+1)
	}
	if len(client.del) > 0 {
		t.Errorf("Expected 1 delete but found %d", len(client.del)+1)
	}
}
