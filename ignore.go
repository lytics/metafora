package metafora

import (
	"container/heap"
	"sync"
	"time"
)

// ignoremgr handles ignoring tasks and sending them back to the consumer once
// their ignore deadline is reached.
type ignoremgr struct {
	incoming chan *timetask
	mu       *sync.RWMutex
	ignores  map[string]struct{}
}

func ignorer(tasks chan<- string, stop <-chan struct{}) *ignoremgr {
	im := &ignoremgr{mu: &sync.RWMutex{}, ignores: make(map[string]struct{}), incoming: make(chan *timetask)}
	go im.monitor(tasks, stop)
	return im
}

func (im *ignoremgr) add(taskID string, until time.Time) {
	// short circuit ignores that have already elapsed
	if until.Before(time.Now()) {
		return
	}
	im.mu.Lock()
	im.ignores[taskID] = struct{}{}
	im.incoming <- &timetask{time: until, task: taskID}
	im.mu.Unlock()
}

func (im *ignoremgr) ignored(taskID string) (ignored bool) {
	im.mu.RLock()
	_, ok := im.ignores[taskID]
	im.mu.RUnlock()

	return ok
}

func (im *ignoremgr) monitor(tasks chan<- string, stop <-chan struct{}) {
	times := timeheap{}
	heap.Init(&times)
	var next *timetask
	for {
		if times.Len() > 0 {
			// Get next ignore from the ignore heap
			next = heap.Pop(&times).(*timetask)
		} else {
			// No ignores! Wait for one to come in or an exit signal
			select {
			case <-stop:
				return
			case newtask := <-im.incoming:
				next = newtask
			}
		}

		timer := time.NewTimer(next.time.Sub(time.Now()))

		select {
		case newtask := <-im.incoming:
			heap.Push(&times, newtask)
			heap.Push(&times, next)
			timer.Stop()
		case <-timer.C:
			// Ignore expired, remove the entry
			im.mu.Lock()
			delete(im.ignores, next.task)
			im.mu.Unlock()

			// Notify the consumer
			select {
			case tasks <- next.task:
			case <-stop:
				return
			}
		case <-stop:
			return
		}
	}
}

func (im *ignoremgr) all() []string {
	im.mu.RLock()
	defer im.mu.RUnlock()
	ignores := make([]string, len(im.ignores))
	i := 0
	for k := range im.ignores {
		ignores[i] = k
		i++
	}
	return ignores
}

type timetask struct {
	time time.Time
	task string
}

// timeheap is a min-heap of time/task tuples sorted by time.
type timeheap []*timetask

func (h timeheap) Len() int           { return len(h) }
func (h timeheap) Less(i, j int) bool { return h[i].time.Before(h[j].time) }
func (h timeheap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timeheap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*timetask))
}

func (h *timeheap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
