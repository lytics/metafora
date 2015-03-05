package metafora

import (
	"testing"
	"time"
)

var (
	_ BalancerContext = (*TestConsumerState)(nil)
	_ ClusterState    = (*TestClusterState)(nil)
)

func TestFairBalancerOneNode(t *testing.T) {
	t.Parallel()
	// Single node should never release tasks
	clusterstate := &TestClusterState{
		Current: map[string]int{"node1": 5},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
	}

	fb := NewDefaultFairBalancer("node1", clusterstate)
	fb.Init(consumerstate)

	if !fb.CanClaim("23") {
		t.Fatal("Expected claim to be true")
	}

	rebalance := fb.Balance()
	if len(rebalance) != 0 {
		t.Fatalf("Expected 0 rebalance tasks: %v", rebalance)
	}
}

func TestFairBalanceOver(t *testing.T) {
	t.Parallel()
	clusterstate := &TestClusterState{
		Current: map[string]int{
			"node1": 10,
			"node2": 2,
		},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
	}

	fb := NewDefaultFairBalancer("node1", clusterstate)
	fb.Init(consumerstate)

	if !fb.CanClaim("23") {
		t.Fatal("Expected claim to be true")
	}

	expect := 2
	rebalance := fb.Balance()
	if len(rebalance) != expect {
		t.Fatalf("Expected %d rebalanced tasks, received %d", expect, len(rebalance))
	}
}

func TestFairBalanceNothing(t *testing.T) {
	t.Parallel()
	clusterstate := &TestClusterState{
		Current: map[string]int{
			"node1": 2,
			"node2": 10,
		},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
	}

	fb := NewDefaultFairBalancer("node1", clusterstate)
	fb.Init(consumerstate)

	if !fb.CanClaim("23") {
		t.Fatal("Expected claim to be true")
	}

	expect := 0
	rebalance := fb.Balance()
	if len(rebalance) != expect {
		t.Fatalf("Expected %d rebalanced tasks, received %d", expect, len(rebalance))
	}

}

type TestClusterState struct {
	Current map[string]int
	Err     error
}

func (ts *TestClusterState) NodeTaskCount() (map[string]int, error) {
	if ts.Err != nil {
		return nil, ts.Err
	}

	return ts.Current, nil
}

type TestConsumerState struct {
	Current []string
}

func (tc *TestConsumerState) Tasks() []Task {
	tasks := []Task{}
	for _, id := range tc.Current {
		tasks = append(tasks, newTask(id, nil))
	}
	return tasks
}

// Sleepy Balancer Tests

type sbCtx struct {
	t     *testing.T
	tasks []string
}

func (ctx *sbCtx) Tasks() []Task {
	tasks := []Task{}
	for _, id := range ctx.tasks {
		tasks = append(tasks, newTask(id, nil))
	}
	return tasks
}
func (ctx *sbCtx) Log(l LogLevel, v string, args ...interface{}) {
	ctx.t.Logf(l.String()+" "+v, args)
}

func TestSleepBalancer(t *testing.T) {
	t.Parallel()
	c := &sbCtx{t: t, tasks: make([]string, 0, 10)}

	b := &SleepBalancer{}
	b.Init(c)

	task := "test-task"
	pre := time.Now()
	total := 0
	for i := 0; i < 10; i++ {
		total += i
		b.CanClaim(task)
		c.tasks = append(c.tasks, task)
	}
	post := time.Now()
	minimum := pre.Add(time.Duration(total) * sleepBalLen)

	// Sleep balancer should never finish before the minimum timeout threshold
	if post.Before(minimum) {
		t.Fatalf("SleepBalancer finished too early: %s < %s", post, minimum)
	}

	// Sleep balancer shouldn't experience much overhead
	if post.After(minimum.Add(50 * time.Millisecond)) {
		t.Fatalf("SleepBalancer went a worrying amount over the expected time: %s > %s", post, minimum)
	}
}
