package metafora

import (
	"testing"
	"time"
)

func TestIgnore(t *testing.T) {
	t.Parallel()
	out := make(chan string)
	stop := make(chan struct{})
	defer close(stop)

	// Create ignorer
	im := ignorer(out, stop)

	// Ignore task for 200ms. Yes this is racy. Might need to bump deadline.
	deadline1 := time.Now().Add(300 * time.Millisecond)
	im.add("1", deadline1)

	// Ensure it's ignored
	if !im.ignored("1") {
		t.Fatal("test task should have been ignored but wasn't")
	}

	// Ignore task for 10ms to make sure tasks are returned in order (they aren't
	// *guaranteed* to be in order since adds and evictions are concurrent)
	deadline2 := time.Now().Add(10 * time.Millisecond)
	im.add("2", deadline2)

	// Wait for the first eviction
	eviction := <-out
	if eviction != "2" {
		t.Fatal("Expected 2 to be evicted before 1")
	}
	now := time.Now()
	if now.Before(deadline2) {
		t.Fatalf("First eviction happened too soon: %s < %s", now, deadline2)
	}

	eviction = <-out
	if eviction != "1" {
		t.Fatal("Expected 1 to be evicted second, found ", eviction)
	}
	now = time.Now()
	if now.Before(deadline1) {
		t.Fatalf("First eviction happened too soon: %s < %s", now, deadline1)
	}
}
