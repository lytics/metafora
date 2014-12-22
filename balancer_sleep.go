package metafora

import "time"

/*
Q. Why 30ms?

A. It's sufficiently long that unless a node is under heavy load (either
computational, GC-induced, or network latency) it should win the claim-race
against nodes with more tasks. If it's under so much load that it loses against
nodes with more tasks, it's probably best to let those other nodes win!

30ms should scale fairly well up to hundreds of tasks per node as Metafora
isn't really intended for high-throughput/low-latency tasks churn.
*/
const sleepBalLen = 30 * time.Millisecond

// SleepBalancer is a simplistic Balancer implementation which sleeps 30ms per
// claimed task in its CanClaim() method. This means the node with the fewest
// claimed tasks in a cluster should sleep the shortest length of time and win
// the claim race.
//
// It never releases tasks during Balance() calls.
type SleepBalancer struct {
	ctx BalancerContext
}

// Init is called by the Consumer.
func (b *SleepBalancer) Init(ctx BalancerContext) { b.ctx = ctx }

// Balance never returns any tasks for the sleepy balancer.
func (*SleepBalancer) Balance() []string { return nil }

// CanClaim sleeps 30ms per claimed task.
func (b *SleepBalancer) CanClaim(string) bool {
	num := len(b.ctx.Tasks())
	time.Sleep(time.Duration(num) * sleepBalLen)
	return true
}
