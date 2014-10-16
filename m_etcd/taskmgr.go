package m_etcd

import (
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type compareAndSwapper interface {
	CompareAndDelete(key, prevValue string, index uint64) (*etcd.Response, error)
	CompareAndSwap(key, value string, ttl uint64, prevValue string, index uint64) (*etcd.Response, error)
}

// taskManager bumps claims to keep them from expiring and deletes them on
// release.
type taskManager struct {
	ctx   metafora.CoordinatorContext
	cas   compareAndSwapper
	wg    sync.WaitGroup
	tasks map[string]chan struct{} // map of task ID to stop chan
	taskL sync.Mutex               // protect tasks from concurrent access

	ttl      uint64 // seconds
	interval time.Duration
}

func newManager(ctx metafora.CoordinatorContext, cas compareAndSwapper, ttl uint64) *taskManager {
	if ttl == 0 {
		panic("refresher: TTL must be > 0")
	}

	// refresh more often than strictly necessary to be safe
	interval := time.Duration((ttl>>1)+(ttl>>2)) * time.Second
	if ttl == 1 {
		interval = 750 * time.Millisecond
		ctx.Log(metafora.LogLevelWarn, "Dangerously low TTL: %d; consider raising.", ttl)
	}
	return &taskManager{
		ctx:      ctx,
		cas:      cas,
		tasks:    make(map[string]chan struct{}),
		ttl:      ttl,
		interval: interval,
	}
}

// add starts refreshing a given key+value pair for a task asynchronously.
func (m *taskManager) add(taskID, key, value string) {
	stop := make(chan struct{})
	m.taskL.Lock()
	m.tasks[taskID] = stop
	m.taskL.Unlock()
	m.wg.Add(1)

	m.ctx.Log(metafora.LogLevelDebug, "Starting claim refresher for task %s", taskID)
	go func() {
		defer func() {
			m.taskL.Lock()
			delete(m.tasks, taskID)
			m.taskL.Unlock()
			m.wg.Done()
		}()

		for {
			select {
			case <-time.After(m.interval):
			case <-stop:
				m.ctx.Log(metafora.LogLevelDebug, "Stopping refresher for task %s", taskID)
				// Try to clean up after ourselves on a clean exit
				if _, err := m.cas.CompareAndDelete(key, value, 0); err != nil {
					m.ctx.Log(metafora.LogLevelWarn, "Error deleting task %s while stopping: %v", taskID, err)
				}
				return
			}
			// 0 index means compare by value
			if _, err := m.cas.CompareAndSwap(key, value, m.ttl, value, 0); err != nil {
				m.ctx.Log(metafora.LogLevelError, "Error trying to update task %s ttl: %v", taskID, err)
				m.ctx.Lost(taskID)
				// On errors, don't even try to Delete as we're in a bad state
				return
			}
		}
	}()
}

// remove tells a single task's refresher to stop.
func (m *taskManager) remove(taskID string) {
	m.taskL.Lock()
	defer m.taskL.Unlock()
	stop, ok := m.tasks[taskID]
	if !ok {
		m.ctx.Log(metafora.LogLevelDebug, "Cannot remove task %s from refresher: not present.", taskID)
		return
	}
	select {
	case <-stop:
		// already closed
	default:
		close(stop)
	}
}

// stop blocks until all refreshers have exited.
func (m *taskManager) stop() {
	func() {
		m.taskL.Lock()
		defer m.taskL.Unlock()
		for _, stop := range m.tasks {
			select {
			case <-stop:
				// already closed
			default:
				close(stop)
			}
		}
	}()
	m.wg.Wait()
}
