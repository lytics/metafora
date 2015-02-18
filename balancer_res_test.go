package metafora

import "testing"

type fakeReporter struct {
	used  uint64
	total uint64
}

func (r *fakeReporter) Used() (uint64, uint64) { return r.used, r.total }
func (r *fakeReporter) String() string         { return "fakes" }

func TestResourceBalancer(t *testing.T) {
	t.Parallel()

	fr := &fakeReporter{used: 750, total: 1000}
	_, err := NewResourceBalancer(fr, 80, 75)
	if err == nil {
		t.Fatal("Expected an error: release threshold was lower than claim.")
	}

	bal, err := NewResourceBalancer(fr, 80, 90)
	if err != nil {
		t.Fatalf("Unexpected error creating resource balancer: %v", err)
	}

	ctx := &TestConsumerState{
		Current: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"},
		Logger:  LogT(t),
	}
	bal.Init(ctx)

	release := bal.Balance()
	if len(release) > 0 {
		t.Errorf("Released tasks when we were well below limits! %v", release)
	}

	// Bump resource usage and rebalance
	fr.used = 901
	release = bal.Balance()
	if len(release) != 1 && release[0] == "1" {
		t.Errorf("Expected 1 released task but found: %v", release)
	}

	// Make sure we scale up the number we release proportionally
	fr.used = 999
	release = bal.Balance()
	if len(release) != 1 && release[0] == "1" {
		t.Errorf("Expected 1 released task but found: %v", release)
	}

	//FIXME When #93 is fixed this test should break as CanClaim should actually
	//      return false
	if !bal.CanClaim("claimmepls") {
		t.Errorf("Until #93 is fixed, CanClaim should always return true")
	}
}
