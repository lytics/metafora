package metafora

import (
	"net/url"
	"testing"

	"github.com/lytics/metafora"
)

func TestSelectDecoratorWithFairBalancerOneNode(t *testing.T) {
	const NodeId = "node1"
	const taskIdNoSels = "32"
	const taskIdWithSels = "42"
	const taskIdWithSelsThatDontMatchAnyLabels = "2003"

	// Single node should never release tasks
	clusterstate := &TestClusterState{
		Current: map[string]int{NodeId: 5},
	}

	//Labels for this node
	labels := make(url.Values)
	labels.Set("node_name", "graywind")
	labels.Set("binary_version", "2.12.66")
	labels.Set("fastnetwork", "1")
	labels.Set("highmemory", "1")

	//selectors by taskid. These are associated with a task, to insure the task lands on a node of your choice.
	selectors := map[string]Selector{
		taskIdWithSels:                       `eq(fastnetwork,"1") AND eq(highmemory,"1") AND contains(binary_version,2.12)`,
		taskIdWithSelsThatDontMatchAnyLabels: `eq(nvidia_gpus,"1") AND eq(highmemory,"1") AND contains(binary_version,2.12)`,
	}

	mclient := &testMetadataClient{
		sels: selectors,
		labs: map[string]url.Values{
			NodeId: labels,
		},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
		newTestlogger(t),
	}

	fb := metafora.NewDefaultFairBalancer(NodeId, clusterstate)
	fb.Init(consumerstate)

	sb := WrapBalancerAsSelectable(NodeId, fb, clusterstate, mclient)

	//
	//Test that this tasks return the correct Claim:
	//
	if !sb.CanClaim(taskIdNoSels) {
		t.Fatal("Expected claim to be true, the task had no selectors")
	}

	if !sb.CanClaim(taskIdWithSels) {
		t.Fatal("Expected claim to be true, the task had selectors that all match")
	}

	if sb.CanClaim(taskIdWithSelsThatDontMatchAnyLabels) {
		//nvidia_gpus should have failed.
		t.Fatal("Expected claim to be false, this task has selectors that shouldn't have matched any values.")
	}

}

///////////////////////////////////////////////////////////////////////////////
//  Below this line are test case implementation for any interfaces needed

///////////////////////////////////////
// decorators.MetadataClient
type testMetadataClient struct {
	sels map[string]Selector
	labs map[string]url.Values
}

func (md *testMetadataClient) Selector(taskid string) Selector {
	return md.sels[taskid]
}
func (md *testMetadataClient) Labels(nodeid string) url.Values {
	return md.labs[nodeid]
}

///////////////////////////////////////
// metafora.ClusterState interface
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

///////////////////////////////////////
// metafora.ConsumerState interface
type TestConsumerState struct {
	Current []string
	metafora.Logger
}

func (tc *TestConsumerState) Tasks() []string {
	return tc.Current
}

///////////////////////////////////////
// metafora.Logger interface
type testlogger struct {
	t *testing.T
}

func newTestlogger(t *testing.T) *testlogger {
	return &testlogger{t: t}
}

func (l *testlogger) Log(lvl metafora.LogLevel, msg string, args ...interface{}) {
	l.t.Logf(msg, args...)
}
