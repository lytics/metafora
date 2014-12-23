package embedded

import (
	"sync"
	"testing"
	"time"

	"github.com/lytics/metafora"
)

func TestEmbedded(t *testing.T) {

	tc := newTestCounter()
	adds := make(chan string, 4)

	addFunc := func(s string) {
		tc.Add(s)
		adds <- s
	}

	thfunc := func() metafora.Handler {
		return newTestHandler(addFunc)
	}

	coord, client := NewEmbeddedPair("testnode")
	runner, _ := metafora.NewConsumer(coord, thfunc, &metafora.DumbBalancer{})

	go func() {
		runner.Run()
	}()

	for _, taskid := range []string{"one", "two", "three", "four"} {
		err := client.SubmitTask(taskid)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(adds) == 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(adds) != 4 {
		t.Errorf("Handlers didn't run in expected amount of time")
	}
	runner.Shutdown()

	runs := tc.Runs()
	if len(runs) != 4 {
		t.Fatalf("Expected 4 runs, got %d", len(runs))
	}

}

func TestEmbeddedStopTask(t *testing.T) {

	testcounter := newTestCounter()
	thfunc := func() metafora.Handler {
		return &blockingtesthandler{make(chan struct{}), testcounter}
	}

	coord, client := NewEmbeddedPair("testnode")
	runner, _ := metafora.NewConsumer(coord, thfunc, &metafora.DumbBalancer{})

	go func() {
		runner.Run()
	}()

	tasks := []string{"one", "two", "three", "four"}

	for _, taskid := range tasks {
		err := client.SubmitTask(taskid)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	for _, taskid := range tasks {
		err := client.DeleteTask(taskid)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	runner.Shutdown()
	if len(testcounter.Runs()) != 4 {
		t.Fatalf("Expected 4 runs, got %d by deadline", len(testcounter.Runs()))
	}
}

func newTestCounter() *testcounter {
	return &testcounter{runs: []string{}}
}

type testcounter struct {
	runs []string
	cmut sync.Mutex
}

func (t *testcounter) Add(r string) {
	t.cmut.Lock()
	defer t.cmut.Unlock()
	t.runs = append(t.runs, r)
}

func (t *testcounter) Runs() []string {
	t.cmut.Lock()
	defer t.cmut.Unlock()
	return t.runs
}

// Run a single function; assumes the function returns (nearly) immediately
func newTestHandler(hfunc func(string)) metafora.Handler {
	return &testhandler{hfunc}
}

type testhandler struct {
	addfunc func(r string)
}

func (th *testhandler) Run(taskId string) (done bool) {
	th.addfunc(taskId)
	return true
}

func (th *testhandler) Stop() {
}

// Blocks until stop is called
type blockingtesthandler struct {
	stopchan chan struct{}
	tc       *testcounter
}

func (bh *blockingtesthandler) Run(taskId string) (done bool) {
	select {
	case <-bh.stopchan:
		bh.tc.Add(taskId)
	case <-time.After(time.Second * 3):
		return false
	}
	return true
}

func (bh *blockingtesthandler) Stop() {
	close(bh.stopchan)
}
