package metafora

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const claimTestTimeout = 5 * time.Second

// gatedBalancer accepts every task, but CanClaim for the gated task ID blocks
// until release is closed.
type gatedBalancer struct {
	gated       string
	entered     chan string
	release     chan struct{}
	releaseOnce sync.Once

	mu    sync.Mutex
	calls map[string]int
}

func newGatedBalancer(gated string) *gatedBalancer {
	return &gatedBalancer{
		gated:   gated,
		entered: make(chan string, 100),
		release: make(chan struct{}),
		calls:   map[string]int{},
	}
}

func (b *gatedBalancer) Init(BalancerContext) {}
func (b *gatedBalancer) Balance() []string    { return nil }

func (b *gatedBalancer) CanClaim(task Task) (time.Time, bool) {
	b.mu.Lock()
	b.calls[task.ID()]++
	b.mu.Unlock()
	b.entered <- task.ID()
	if task.ID() == b.gated {
		<-b.release
	}
	return NoDelay, true
}

func (b *gatedBalancer) open() { b.releaseOnce.Do(func() { close(b.release) }) }

func (b *gatedBalancer) callsFor(id string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[id]
}

func expectID(t *testing.T, ch <-chan string, want, what string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("%s: got %q, want %q", what, got, want)
		}
	case <-time.After(claimTestTimeout):
		t.Fatalf("%s: %q never arrived", what, want)
	}
}

func startConsumer(t *testing.T, coord Coordinator, bal Balancer) (*Consumer, <-chan string) {
	t.Helper()
	hf, tasksRun := newTestHandlerFunc(t)
	c, err := NewConsumer(coord, hf, bal)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	go c.Run()
	return c, tasksRun
}

func TestConsumerKeepsReadingTasksWhileBalancerSleeps(t *testing.T) {
	t.Parallel()
	tc := NewTestCoord()
	bal := newGatedBalancer("slow")
	c, tasksRun := startConsumer(t, tc, bal)
	defer c.Shutdown()
	defer bal.open()

	tc.Tasks <- testTask{"slow"}
	expectID(t, bal.entered, "slow", "CanClaim")
	tc.Tasks <- testTask{"fast"}
	expectID(t, tasksRun, "fast", "task run while another task's CanClaim blocks")

	bal.open()
	expectID(t, tasksRun, "slow", "task run once its CanClaim returns")
}

func TestConsumerAsksBalancerOncePerAwaitingTask(t *testing.T) {
	t.Parallel()
	tc := NewTestCoord()
	bal := newGatedBalancer("dup")
	c, tasksRun := startConsumer(t, tc, bal)
	defer c.Shutdown()
	defer bal.open()

	tc.Tasks <- testTask{"dup"}
	expectID(t, bal.entered, "dup", "CanClaim")
	tc.Tasks <- testTask{"dup"}
	// The watch delivers in order, so once marker runs the repeat of dup has been handled.
	tc.Tasks <- testTask{"marker"}
	expectID(t, tasksRun, "marker", "task run")

	bal.open()
	expectID(t, tasksRun, "dup", "task run once its CanClaim returns")
	if n := bal.callsFor("dup"); n != 1 {
		t.Errorf("CanClaim called %d times for a task delivered twice while awaiting it, want 1", n)
	}
}

// serialCheckCoord records the most Claim calls it ever saw in flight at once.
type serialCheckCoord struct {
	*TestCoord
	inflight, maxInflight atomic.Int32
}

func (c *serialCheckCoord) Claim(Task) bool {
	n := c.inflight.Add(1)
	for m := c.maxInflight.Load(); n > m && !c.maxInflight.CompareAndSwap(m, n); m = c.maxInflight.Load() {
	}
	time.Sleep(5 * time.Millisecond)
	c.inflight.Add(-1)
	return true
}

func TestConsumerClaimsSerially(t *testing.T) {
	t.Parallel()
	coord := &serialCheckCoord{TestCoord: NewTestCoord()}
	c, tasksRun := startConsumer(t, coord, DumbBalancer)
	defer c.Shutdown()

	ids := []string{"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9"}
	for _, id := range ids {
		coord.Tasks <- testTask{id}
	}
	for range ids {
		select {
		case <-tasksRun:
		case <-time.After(claimTestTimeout):
			t.Fatal("tasks did not all run")
		}
	}
	if m := coord.maxInflight.Load(); m != 1 {
		t.Errorf("%d Claim calls were in flight at once, want 1", m)
	}
}

func TestConsumerShutdownDoesNotWaitForBalancer(t *testing.T) {
	t.Parallel()
	tc := NewTestCoord()
	bal := newGatedBalancer("stuck")
	c, _ := startConsumer(t, tc, bal)
	defer bal.open()

	tc.Tasks <- testTask{"stuck"}
	expectID(t, bal.entered, "stuck", "CanClaim")

	done := make(chan struct{})
	go func() {
		c.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(claimTestTimeout):
		t.Fatal("Shutdown waited on a blocked CanClaim")
	}
}
