package metafora

import (
	"fmt"
	"time"
)

// ResourceReporter is required by the ResourceBalancer to read the resource
// being used for balancing.
type ResourceReporter interface {
	// Used returns the amount of a resource used and the total amount of that
	// resource.
	Used() (used uint64, total uint64)

	// String returns the plural name of the unit resources are reported in.
	String() string
}

// ResourceBalancer is a balancer implemntation which uses two thresholds to
// limit claiming and rebalance work based upon a resource reported by a
// ResourceReporter. When the claim threshold is exceeded, no new work will be
// claimed. When the release threshold is exceeded work will be released until
// below that threshold. The claim threshold must be less than the release
// threshold (otherwise claims would continue just to have the work
// rebalanced.)
//
// Even below the claim limit, claims are delayed by the percent of resources
// used (in milliseconds) to give less loaded nodes a claim advantage.
//
// The balancer releases the oldest tasks first (skipping those who are already
// stopping) to try to prevent rebalancing the same tasks repeatedly within a
// cluster.
type ResourceBalancer struct {
	ctx      BalancerContext
	reporter ResourceReporter

	claimLimit   int
	releaseLimit int
}

// NewResourceBalancer creates a new ResourceBalancer or returns an error if
// the limits are invalid.
func NewResourceBalancer(src ResourceReporter, claimLimit, releaseLimit int) (*ResourceBalancer, error) {
	if claimLimit < 1 || claimLimit > 100 || releaseLimit < 1 || releaseLimit > 100 {
		return nil, fmt.Errorf("Limits must be between 1 and 100. claim=%d release=%d", claimLimit, releaseLimit)
	}
	if claimLimit >= releaseLimit {
		return nil, fmt.Errorf("Claim threshold must be < release threshold. claim=%d >= release=%d", claimLimit, releaseLimit)
	}

	return &ResourceBalancer{
		reporter:     src,
		claimLimit:   claimLimit,
		releaseLimit: releaseLimit,
	}, nil
}

func (b *ResourceBalancer) Init(ctx BalancerContext) {
	b.ctx = ctx
}

func (b *ResourceBalancer) CanClaim(string) bool {
	used, total := b.reporter.Used()
	threshold := int(float32(used) / float32(total) * 100)
	if threshold >= b.claimLimit {
		//FIXME Until #93 is fixed returning false is very dangerous as it could
		//      cause a tight loop with the coordinator. Sleep longer than more
		//      lightly loaded nodes.
		dur := time.Duration(100+(threshold-b.claimLimit)) * time.Millisecond
		b.ctx.Log(LogLevelInfo, "%d is over the claim limit of %d. Used %d of %d %s. Sleeping %s before claiming.",
			threshold, b.claimLimit, used, total, b.reporter, dur)
		time.Sleep(dur)
		return true
	}

	// Always sleep based on resource usage to give less loaded nodes an advantage
	dur := time.Duration(threshold) * time.Millisecond
	time.Sleep(dur)
	return true
}

func (b *ResourceBalancer) Balance() []string {
	used, total := b.reporter.Used()
	threshold := int(float32(used) / float32(total) * 100)
	if threshold < b.releaseLimit {
		// We're below the limit! Don't release anything.
		return nil
	}

	// Release the oldest task that isn't already stopping
	var task Task
	for _, t := range b.ctx.Tasks() {
		if t.Stopped().IsZero() && (task == nil || task.Started().After(t.Started())) {
			task = t
		}
	}

	// No tasks or all tasks are stopping, don't bother rebalancing
	if task == nil {
		return nil
	}

	b.ctx.Log(LogLevelInfo, "Releasing task %s (started %s) because %d > %d (%d of %d %s used)",
		task.ID(), task.Started(), threshold, b.releaseLimit, used, total, b.reporter)
	return []string{task.ID()}
}
