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
	stop     <-chan struct{}

	mu      *sync.RWMutex
	ignores map[string]struct{}
}

func ignorer(tasks chan<- Task, stop <-chan struct{}) *ignoremgr {
	im := &ignoremgr{
		incoming: make(chan *timetask),
		stop:     stop,
		mu:       &sync.RWMutex{},
		ignores:  make(map[string]struct{}),
	}
	go im.monitor(tasks, stop)
	return im
}

func (im *ignoremgr) add(task Task, until time.Time) {
	// short circuit zero times; queue everything else
	if until.IsZero() {
		return
	}

	// Add to ignore map
	im.mu.Lock()
	im.ignores[task.ID()] = struct{}{}
	im.mu.Unlock()

	// Send to monitor for pushing onto time heap
	select {
	case im.incoming <- &timetask{time: until, task: task}:
	case <-im.stop:
		// Don't bother adding ignore if we're just exiting
	}
}

func (im *ignoremgr) ignored(taskID string) (ignored bool) {
	im.mu.RLock()
	_, ok := im.ignores[taskID]
	im.mu.RUnlock()

	return ok
}

func (im *ignoremgr) monitor(tasks chan<- Task, stop <-chan struct{}) {
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

		// this duration *may* be negative, in which case the
		// task will be pushed immediately
		timer := time.NewTimer(next.time.Sub(time.Now()))

		select {
		case newtask := <-im.incoming:
			// Push onto next task and new task onto time heap
			heap.Push(&times, newtask)
			heap.Push(&times, next)

			// Stop the existing timer for this loop iteration
			timer.Stop()
		case <-timer.C:
			// Ignore expired, remove the entry
			im.mu.Lock()
			delete(im.ignores, next.task.ID())
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
	task Task
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
