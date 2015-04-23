package metafora

import (
	"testing"
	"time"
)

type releaseAllBalancer struct {
	balances chan int
	ctx      BalancerContext
}

func (b *releaseAllBalancer) Init(c BalancerContext) {
	b.ctx = c
	b.balances = make(chan int)
}
func (b *releaseAllBalancer) CanClaim(string) (time.Time, bool) { return NoDelay, true }
func (b *releaseAllBalancer) Balance() []string {
	b.balances <- 1
	ids := []string{}
	for _, task := range b.ctx.Tasks() {
		ids = append(ids, task.ID())
	}
	return ids
}

func TestDoubleRelease(t *testing.T) {
	t.Parallel()

	started := make(chan int)
	reallyStop := make(chan bool)
	h := SimpleHandler(func(task string, stop <-chan bool) bool {
		started <- 1
		t.Logf("TestDoubleRelease handler recieved %s - blocking until reallyStop closed.", task)
		<-reallyStop
		return true
	})

	tc := NewTestCoord()

	b := &releaseAllBalancer{}
	c, err := NewConsumer(tc, h, b)
	if err != nil {
		t.Fatalf("Error creating consumer: %v", err)
	}
	go c.Run()

	// This won't exit when told to
	tc.Tasks <- "1"
	<-started

	// Make sure balancing/mainloop isn't blocked
	tc.Commands <- CommandBalance()
	<-b.balances
	tc.Commands <- CommandBalance()
	<-b.balances
	tc.Commands <- CommandBalance()
	<-b.balances

	shutdownComplete := make(chan bool)
	go func() {
		c.Shutdown()
		close(shutdownComplete)
	}()

	// Make sure the release insidiously blocks until we close reallyStop
	select {
	case <-shutdownComplete:
		t.Fatal("Shutdown completed when it should have blocked indefinitely")
	case <-time.After(100 * time.Millisecond):
	}

	// Close reallyStop and make sure Shutdown actually exits
	close(reallyStop)
	// Make sure the release insidiously blocks until we close reallyStop
	<-shutdownComplete
}
