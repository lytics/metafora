package metafora

import (
	"testing"
)

var (
	_ BalancerContext = (*TestConsumerState)(nil)
	_ ClusterState    = (*TestClusterState)(nil)
)

func TestFairBalancerOneNode(t *testing.T) {
	// Single node should never release tasks
	clusterstate := &TestClusterState{
		Current: map[string]int{"node1": 5},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
		&logger{},
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
	clusterstate := &TestClusterState{
		Current: map[string]int{
			"node1": 10,
			"node2": 2,
		},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
		&logger{},
	}

	fb := NewDefaultFairBalancer("node1", clusterstate)
	fb.Init(consumerstate)

	if !fb.CanClaim("23") {
		t.Fatal("Expected claim to be true")
	}

	expect := 3
	rebalance := fb.Balance()
	if len(rebalance) != expect {
		t.Fatalf("Expected %d rebalanced tasks, received %d", expect, len(rebalance))
	}
}

func TestFairBalanceNothing(t *testing.T) {
	clusterstate := &TestClusterState{
		Current: map[string]int{
			"node1": 2,
			"node2": 10,
		},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
		&logger{},
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
	Logger
}

func (tc *TestConsumerState) Tasks() []string {
	return tc.Current
}
