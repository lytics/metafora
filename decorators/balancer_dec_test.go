package metafora

import (
	"testing"

	"github.com/lytics/metafora"
)

//TODO Add support for boolean options between selectors.
//
// For cases like:
//   labels contains(highmemory && fastnetwork)
// The current code only supports:
//   labels contains(highmemory || fastnetwork)
// With the || being implicit

func TestSelectDecoratorWithFairBalancerOneNode(t *testing.T) {
	const NodeId = "node1"
	const taskIdNoSels = "32"
	const taskIdWithSels = "42"
	const taskIdWithSelsThatDontMatchAnyLabels = "2003"

	// Single node should never release tasks
	clusterstate := &TestClusterState{
		Current: map[string]int{NodeId: 5},
	}

	mclient := &testMetadataClient{
		sels: map[string]SelectorSet{
			taskIdWithSels: SelectorSet{
				"fastnetwork": nil,
			},
			taskIdWithSelsThatDontMatchAnyLabels: SelectorSet{
				"highmemory": nil,
			},
		},
		labs: map[string]LabelSet{
			NodeId: LabelSet{
				"fastnetwork": nil,
			},
		},
	}

	consumerstate := &TestConsumerState{
		[]string{"1", "2", "3", "4", "5"},
		newTestlogger(t),
	}

	fb := metafora.NewDefaultFairBalancer(NodeId, clusterstate)
	fb.Init(consumerstate)

	sb := WrapBalancerAsSelectable(NodeId, fb, clusterstate, mclient)

	if !sb.CanClaim(taskIdNoSels) {
		t.Fatal("Expected claim to be true")
	}

	if !sb.CanClaim(taskIdWithSels) {
		t.Fatal("Expected claim to be true")
	}

	if sb.CanClaim(taskIdWithSelsThatDontMatchAnyLabels) {
		t.Fatal("Expected claim to be false")
	}

}

///////////////////////////////////////////////////////////////////////////////
//  Below this line are test case implementation for any interfaces needed

///////////////////////////////////////
// decorators.MetadataClient
type testMetadataClient struct {
	sels map[string]SelectorSet
	labs map[string]LabelSet
}

func (md *testMetadataClient) Selectors(taskid string) SelectorSet {
	return md.sels[taskid]
}
func (md *testMetadataClient) Labels(nodeid string) LabelSet {
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
