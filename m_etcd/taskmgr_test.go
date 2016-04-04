package m_etcd

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
)

type fakeEtcd struct {
	t   *testing.T
	add chan string
	del chan string
	cas chan string
	cad chan string
}

func (f *fakeEtcd) Get(ctx context.Context, key string, opts *client.GetOptions) (*client.Response, error) {
	f.t.Logf("Get(%v, %v, %#v)", ctx, key, opts)
	var index uint64 = 1
	if strings.HasSuffix(key, "/zombie") {
		// Testing resurrection, see comment in Create above
		index = 666
	}
	return &client.Response{Node: &client.Node{CreatedIndex: index}}, nil
}

func (f *fakeEtcd) Set(ctx context.Context, key, value string, opts *client.SetOptions) (*client.Response, error) {
	f.t.Logf("Set(%v, %v, %v, %#v)", ctx, key, value, opts)
	if opts.PrevValue != "" {
		if key == "testns/testlost/owner" {
			return nil, fmt.Errorf("test error")
		}
		f.cas <- key
		return nil, nil
	}
	f.add <- key

	// Due to lytics/metafora#124 claims will do a get after a create to make
	// sure the created index of the task directory doesn't match the created
	// index of the claim key. If key=="zombie", fake a resurrected task,
	// otherwise return differing values to avoid triggering this workaround.
	var index uint64 = 2
	if strings.HasSuffix(key, "/zombie/owner") {
		index = 666
	}
	return &client.Response{Node: &client.Node{CreatedIndex: index}}, nil
}

func (f *fakeEtcd) Delete(ctx context.Context, key string, opts *client.DeleteOptions) (*client.Response, error) {
	f.t.Logf("Delete(%v, %v, %#v))", ctx, key, opts)
	if opts.PrevValue != "" {
		f.cad <- key
		return nil, nil
	}
	f.del <- key
	return nil, nil
}

func (f *fakeEtcd) Create(ctx context.Context, key, value string) (*client.Response, error) {
	panic("not implemented")
}

func (f *fakeEtcd) CreateInOrder(ctx context.Context, dir, value string, opts *client.CreateInOrderOptions) (*client.Response, error) {
	panic("not implemented")
}

func (f *fakeEtcd) Update(ctx context.Context, key, value string) (*client.Response, error) {
	panic("not implemented")
}

func (f *fakeEtcd) Watcher(key string, opts *client.WatcherOptions) client.Watcher {
	panic("not implemented")
}

func newFakeEtcd(t *testing.T) *fakeEtcd {
	return &fakeEtcd{
		t:   t,
		add: make(chan string, 100),
		del: make(chan string, 100),
		cas: make(chan string, 100),
		cad: make(chan string, 100),
	}
}

// TestTaskResurrection ensures that attempting to Claim (add) a Done (removed)
// task doesn't succeed. See https://github.com/lytics/metafora/issues/124
func TestTaskResurrection(t *testing.T) {
	t.Parallel()

	client := newFakeEtcd(t)
	const ttl = 2 * time.Second
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)
	if added := mgr.add(&task{id: "zombie"}); added {
		t.Fatal("Added zombie task when it should have been deleted.")
	}
}

// Test that tasks are refreshed periodically.
func TestTaskRefreshing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping due to -short")
	}
	t.Parallel()

	client := newFakeEtcd(t)
	const ttl = 2 * time.Second
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)
	if added := mgr.add(&task{id: "tid"}); !added {
		t.Fatal("Failed to add task!")
	}
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
	t.Parallel()

	client := newFakeEtcd(t)
	const ttl = 2 * time.Second
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)
	mgr.add(&task{id: "tid"})
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
	t.Parallel()

	client := newFakeEtcd(t)
	const ttl = 3 * time.Second
	mgr := newManager(newCtx(t, "mgr"), client, "testns", "testnode", ttl)

	// Add a few tasks and remove one
	mgr.add(&task{id: "tid1"})
	mgr.add(&task{id: "tid2"})
	mgr.add(&task{id: "tid3"})
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
		case <-time.After(2750 * time.Millisecond):
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
	t.Parallel()

	ctx := newCtx(t, "mgr")
	client := newFakeEtcd(t)
	const ttl = 2 * time.Second
	mgr := newManager(ctx, client, "testns", "testnode", ttl)

	mgr.add(&task{id: "testlost"})

	// Wait for the CAS to fail
	time.Sleep(ttl)

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
	client := newFakeEtcd(t)
	const ttl = 2 * time.Second
	mgr := newManager(ctx, client, "testns", "testnode", ttl)

	mgr.add(&task{id: "t1"})
	mgr.add(&task{id: "t2"})
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
