package metafora

import (
	"testing"
	"time"
)

// Handler/Consumer test

type testCoord struct{}

func (*testCoord) Init(Logger)       {}
func (*testCoord) Watch() string     { return "" }
func (*testCoord) Claim(string) bool { return true }
func (*testCoord) Command() string   { return "" }

type testHandler struct {
	stop chan int
	t    *testing.T
	id   string
}

func (h *testHandler) Run(id string) error {
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

func newTestHandlerFunc(t *testing.T) HandlerFunc {
	return func() Handler {
		return &testHandler{
			stop: make(chan int),
			t:    t,
		}
	}
}

func TestConsumer(t *testing.T) {
	c := NewConsumer(&testCoord{}, newTestHandlerFunc(t), &DumbBalancer{})
	c.claimed("test1")
	c.claimed("test2")

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
}

// Balancer/ConsumerState test

type testBalancer struct {
	c ConsumerState
	t *testing.T
}

func (b *testBalancer) Init(c ConsumerState) { b.c = c }
func (b *testBalancer) CanClaim(taskID string) bool {
	return taskID == "ok-task"
}
func (b *testBalancer) Balance() []string {
	tsks := b.c.Tasks()
	if len(tsks) != 1 {
		b.t.Errorf("len(ConsumerState.Tasks()) != 1 ==> %v", tsks)
		return nil
	}
	if tsks[0] != "ok-task" {
		b.t.Errorf("Wrong task in ConsumerState.Tasks(): %v", tsks)
	}
	return nil
}

func TestBalancer(t *testing.T) {
	c := NewConsumer(&testCoord{}, newTestHandlerFunc(t), &testBalancer{})
	c.claimed("test1")
	c.claimed("ok-task")
	c.claimed("test2")

	if r := c.bal.Balance(); len(r) > 0 {
		t.Errorf("Balance() should return 0, not: %v", r)
	}

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
}
