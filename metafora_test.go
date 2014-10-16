package metafora

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// Handler/Consumer test

type testCoord struct {
	tasks    chan string // will be returned in order, "" indicates return an error
	commands chan Command
}

func newTestCoord() *testCoord {
	return &testCoord{tasks: make(chan string, 10), commands: make(chan Command, 10)}
}

func (*testCoord) Init(CoordinatorContext) {}
func (*testCoord) Claim(string) bool       { return true }
func (*testCoord) Close()                  { return }
func (*testCoord) Release(string)          {}

func (c *testCoord) Watch() (string, error) {
	task := <-c.tasks
	if task == "" {
		return "", errors.New("test error")
	}
	return task, nil
}

func (c *testCoord) Command() (Command, error) {
	cmd := <-c.commands
	if cmd == nil {
		return cmd, errors.New("test error")
	}
	return cmd, nil
}

type testHandler struct {
	stop     chan int
	t        *testing.T
	id       string
	tasksRun chan string
}

func (h *testHandler) Run(id string) error {
	h.tasksRun <- id
	h.id = id
	h.t.Logf("Run(%s)", id)
	<-h.stop
	h.t.Logf("Stop received for %s", id)
	return nil
}

func (h *testHandler) Stop() {
	h.t.Logf("Stopping %s", h.id)
	close(h.stop)
}

func newTestHandlerFunc(t *testing.T) (HandlerFunc, chan string) {
	tasksRun := make(chan string, 10)
	return func() Handler {
		return &testHandler{
			stop:     make(chan int),
			t:        t,
			tasksRun: tasksRun,
		}
	}, tasksRun
}

// TestConsumer ensures the consumers main loop properly handles tasks as well
// as errors and Shutdown.
func TestConsumer(t *testing.T) {
	//FIXME hack retry delay for quicker error testing
	origCRD := consumerRetryDelay
	consumerRetryDelay = 10 * time.Millisecond
	defer func() { consumerRetryDelay = origCRD }()

	// Setup some tasks to run in a fake coordinator
	tc := newTestCoord()
	tc.tasks <- "test1"
	tc.tasks <- "" // cause an error which should be a noop
	tc.tasks <- "test2"

	// Setup a handler func that lets us know what tasks are running
	hf, tasksRun := newTestHandlerFunc(t)

	// Create the consumer and run it
	c := NewConsumer(tc, hf, &DumbBalancer{})
	s := make(chan int)
	start := time.Now()
	go func() {
		c.Run()
		s <- 1
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-s:
			t.Fatalf("Run exited early")
		case tr := <-tasksRun:
			if tr != "test1" && tr != "test2" {
				t.Errorf("Expected `test1` or `test2` but received: %s", tr)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("First task didn't execute in a timely fashion")
		}
	}

	//FIXME ensure we waited the retry delay as a way to test for error handling
	if time.Now().Sub(start) < consumerRetryDelay {
		t.Error("Consumer didn't pause before retrying after an error")
	}

	// Ensure Tasks() is accurate
	tasks := c.Tasks()
	if len(tasks) != 2 {
		t.Errorf("Expected 2 tasks to be running but found: %v", tasks)
	}

	go func() {
		c.Shutdown()
		s <- 1
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-s:
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Run and Shutdown didn't finish in a timely fashion")
		}
	}
}

// Balancer/Consumer test

type testBalancer struct {
	c         BalancerContext
	t         *testing.T
	secondRun bool
	done      chan struct{}
}

func (b *testBalancer) Init(c BalancerContext) { b.c = c }
func (b *testBalancer) CanClaim(taskID string) bool {
	b.t.Logf("CanClaim(%s)", taskID)
	return taskID == "ok-task"
}
func (b *testBalancer) Balance() []string {
	if b.secondRun {
		return nil
	}
	b.secondRun = true
	tsks := b.c.Tasks()
	if len(tsks) != 1 {
		b.t.Errorf("len(ConsumerState.Tasks()) != 1 ==> %v", tsks)
		return nil
	}
	if tsks[0] != "ok-task" {
		b.t.Errorf("Wrong task in ConsumerState.Tasks(): %v", tsks)
	}
	close(b.done)
	return nil
}

func TestBalancer(t *testing.T) {
	// ugly hack to force fast rebalancing (use atomic to make race detector happy)
	oldJ := atomic.LoadInt64(&balanceJitterMax)
	atomic.StoreInt64(&balanceJitterMax, 1)
	defer func() { atomic.StoreInt64(&balanceJitterMax, oldJ) }()

	hf, tasksRun := newTestHandlerFunc(t)
	tc := newTestCoord()
	balDone := make(chan struct{})
	c := NewConsumer(tc, hf, &testBalancer{t: t, done: balDone})
	c.balEvery = 10 * time.Millisecond
	go c.Run()
	tc.tasks <- "test1"
	tc.tasks <- "ok-task"
	tc.tasks <- "test2"

	// Wait for balance
	select {
	case <-balDone:
	case <-time.After(c.balEvery * 10):
		t.Error("Didn't balance in a timely fashion")
	}

	select {
	case run := <-tasksRun:
		if run != "ok-task" {
			t.Errorf("Balancer didn't reject tasks properly. Ran task %s", run)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Task didn't run in a timely fashion")
	}

	/*
		if r := c.bal.Balance(); len(r) > 0 {
			t.Errorf("Balance() should return 0, not: %v", r)
		}
	*/

	s := make(chan int)
	go func() {
		c.Shutdown()
		close(s)
	}()
	select {
	case <-s:
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Shutdown didn't finish in a timely fashion")
	}
	if len(c.Tasks()) != 0 {
		t.Errorf("Shutdown didn't stop all tasks")
	}
}
