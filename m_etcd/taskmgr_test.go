package m_etcd

import (
	"fmt"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

type testCAS struct {
	swap chan string
	del  chan string
}

func (t testCAS) CompareAndDelete(k, pv string, _ uint64) (*etcd.Response, error) {
	t.del <- k
	return nil, nil
}
func (t testCAS) CompareAndSwap(k, v string, ttl uint64, pv string, _ uint64) (*etcd.Response, error) {
	if k == "path/to/testlost" {
		return nil, fmt.Errorf("test error")
	}
	t.swap <- k
	return nil, nil
}
func newCAS() testCAS {
	return testCAS{
		swap: make(chan string, 10),
		del:  make(chan string, 10),
	}
}

// Test that tasks are refreshed periodically.
func TestTaskRefreshing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	cas := newCAS()
	const ttl = 2
	mgr := newManager(newCtx(t, "mgr"), cas, ttl)
	defer mgr.stop()
	mgr.add("tid", "path/to/tid", "value")
	for i := 0; i < 2; i++ {
		select {
		case <-cas.swap:
			t.Log("Task refreshed.")
		case <-cas.del:
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

	cas := newCAS()
	const ttl = 2
	mgr := newManager(newCtx(t, "mgr"), cas, ttl)
	defer mgr.stop()
	mgr.add("tid", "path/to/tid", "value")
	mgr.remove("tid")
	select {
	case <-cas.del:
		// Yay, everything worked
	case <-time.After(1 * time.Second):
		t.Errorf("Task wasn't removed soon enough.")
	}

	select {
	case <-cas.swap:
		t.Errorf("Task shouldn't have lived long enough to be CAS'd")
	default:
	}
}

// Test multiple tasks can be added, will be refreshed, removed, and stopped.
func TestFullTaskMgr(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	cas := newCAS()
	const ttl = 2
	mgr := newManager(newCtx(t, "mgr"), cas, ttl)
	defer mgr.stop()

	// Add a few tasks and remove one
	mgr.add("tid1", "path/to/tid1", "value")
	mgr.add("tid2", "path/to/tid2", "value")
	mgr.add("tid3", "path/to/tid3", "value")
	mgr.remove("tid2")

	delDone := false
	expectedSwaps := map[string]bool{"path/to/tid1": true, "path/to/tid3": true}

	// Test that only expected actions occurred (and in a timely manner)
	for i := 0; i < 3; i++ {
		select {
		case path := <-cas.swap:
			if !expectedSwaps[path] {
				t.Errorf("CAS'd unexpected task: %s", path)
			}
			delete(expectedSwaps, path)
		case path := <-cas.del:
			if path != "path/to/tid2" {
				t.Errorf("Deleted unexpected task: %s", path)
			}
			if delDone {
				t.Errorf("Deleted tid2 twice!")
			}
			delDone = true
		case <-time.After(1500 * time.Millisecond):
			t.Errorf("Took too long for refreshes to happen")
		}
	}

	// Calling remove and stop concurrently should be safe
	go mgr.remove("tid3")
	go mgr.stop()
	expectedDels := map[string]bool{"path/to/tid1": true, "path/to/tid3": true}
	for len(expectedDels) > 0 {
		select {
		case path := <-cas.del:
			if !expectedDels[path] {
				t.Errorf("Removed unexpected task: %s", path)
			}
			delete(expectedDels, path)
		case <-time.After(1 * time.Second):
			t.Errorf("Took too long for deletes to happen")
		}
	}

	// Stopping more than once is silly but should be a safe noop
	mgr.stop()
	if len(cas.del) > 0 {
		t.Errorf("Unexpected deletes occurred")
	}
}

// Test that losing tasks who cannot be refreshed works.
func TestTaskLost(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}

	ctx := newCtx(t, "mgr")
	cas := newCAS()
	const ttl = 2
	mgr := newManager(ctx, cas, ttl)
	defer mgr.stop()
	mgr.add("testlost", "path/to/testlost", "value")

	// Wait for the CAS to fail
	time.Sleep(ttl * time.Second)

	if len(cas.swap) > 0 {
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
	mgr.remove("testlost")
	// as should stopping
	mgr.stop()

	if len(cas.del) > 0 {
		t.Error("Unexpectedly deleted non-existant tasks when shutting down.")
	}
}
