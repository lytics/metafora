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

	thfunc := metafora.SimpleHandler(func(id string, _ <-chan bool) bool {
		tc.Add(id)
		adds <- id
		return true
	})

	coord, client := NewEmbeddedPair("testnode")
	runner, _ := metafora.NewConsumer(coord, thfunc, &metafora.DumbBalancer{})

	go runner.Run()

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

func TestEmbeddedShutdown(t *testing.T) {
	const n = 4
	runs := make(chan int, n)
	stops := make(chan int, n)
	thfunc := metafora.SimpleHandler(func(id string, s <-chan bool) bool {
		runs <- 1
		select {
		case <-s:
			stops <- 1
			return false
		case <-time.After(time.Second * 3):
			return true
		}
	})

	coord, client := NewEmbeddedPair("testnode")
	runner, _ := metafora.NewConsumer(coord, thfunc, &metafora.DumbBalancer{})

	go runner.Run()

	// len(tasks) must == n
	tasks := []string{"one", "two", "three", "four"}

	// submit tasks
	for _, taskid := range tasks {
		err := client.SubmitTask(taskid)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	}

	// make sure all 4 start
	for i := 0; i < n; i++ {
		<-runs
	}

	// tell them to stop
	runner.Shutdown()

	// make sure all 4 stop
	for i := 0; i < n; i++ {
		<-stops
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
