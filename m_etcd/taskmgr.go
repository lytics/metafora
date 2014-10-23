package m_etcd

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

// Don't depend directly on etcd.Client to make testing easier.
type client interface {
	Create(key, value string, ttl uint64) (*etcd.Response, error)
	Delete(key string, recursive bool) (*etcd.Response, error)
	CompareAndDelete(key, prevValue string, index uint64) (*etcd.Response, error)
	CompareAndSwap(key, value string, ttl uint64, prevValue string, index uint64) (*etcd.Response, error)
}

type taskExit struct {
	done bool
}

// Done returns true if the task is being stopped because it is completed. The
// claim refresher may try to CAS or CAD the claim key while the underlying
// task directory is being deleted, so this flag let's them know to ignore
// failures in that case.
func (t taskExit) Done() bool { return t.done }

// taskManager bumps claims to keep them from expiring and deletes them on
// release.
type taskManager struct {
	ctx    metafora.CoordinatorContext
	client client
	wg     sync.WaitGroup
	tasks  map[string]chan taskExit // map of task ID to stop chan
	taskL  sync.Mutex               // protect tasks from concurrent access
	path   string                   // etcd path to tasks
	node   string                   // node ID

	ttl      uint64 // seconds
	interval time.Duration
}

func newManager(ctx metafora.CoordinatorContext, client client, path, nodeID string, ttl uint64) *taskManager {
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
		client:   client,
		tasks:    make(map[string]chan taskExit),
		path:     path,
		node:     nodeID,
		ttl:      ttl,
		interval: interval,
	}
}

func (m *taskManager) taskPath(taskID string) string {
	return path.Join(m.path, taskID)
}

func (m *taskManager) ownerKey(taskID string) string {
	return path.Join(m.taskPath(taskID), OwnerMarker)
}

func (m *taskManager) ownerNode(taskID string) (key, value string) {
	p, err := json.Marshal(&ownerValue{Node: m.node})
	if err != nil {
		panic(fmt.Sprintf("coordinator: error marshalling node body: %v", err))
	}
	return m.ownerKey(taskID), string(p)
}

// add starts refreshing a given key+value pair for a task asynchronously.
func (m *taskManager) add(taskID string) bool {
	// Attempt to claim the node
	key, value := m.ownerNode(taskID)
	_, err := m.client.Create(key, value, m.ttl)
	if err != nil {
		etcdErr, ok := err.(*etcd.EtcdError)
		if !ok || etcdErr.ErrorCode != EcodeNodeExist {
			m.ctx.Log(metafora.LogLevelError, "Claim of %s failed with an expected error: %v", key, err)
		} else {
			m.ctx.Log(metafora.LogLevelInfo, "Claim of %s failed, already claimed", key)
		}
		return false
	}

	// Claim successful, start the refresher
	m.ctx.Log(metafora.LogLevelDebug, "Claim successful: %s", key)
	// buffer stop messages so senders don't block
	stop := make(chan taskExit, 1)
	m.taskL.Lock()
	m.tasks[taskID] = stop
	m.taskL.Unlock()

	m.ctx.Log(metafora.LogLevelDebug, "Starting claim refresher for task %s", taskID)
	m.wg.Add(1)
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
				// Try to refresh the claim node (0 index means compare by value)
				if _, err := m.client.CompareAndSwap(key, value, m.ttl, value, 0); err != nil {
					select {
					case reason := <-stop:
						if reason.Done() {
							// CAS failed because the task was done and the directory was removed
							// out from underneath the task manager.  Exit cleanly.
							return
						}
					default:
					}
					m.ctx.Log(metafora.LogLevelError, "Error trying to update task %s ttl: %v", taskID, err)
					m.ctx.Lost(taskID)
					// On errors, don't even try to Delete as we're in a bad state
					return
				}
			case reason := <-stop:
				m.ctx.Log(metafora.LogLevelDebug, "Stopping refresher for task %s", taskID)
				if reason.Done() {
					// Done, delete the entire task directory
					const recursive = true
					if _, err := m.client.Delete(m.taskPath(taskID), recursive); err != nil {
						m.ctx.Log(metafora.LogLevelError, "Error deleting task %s while stopping: %v", taskID, err)
					}
				} else {
					// Not done, releasing; just delete the claim node
					if _, err := m.client.CompareAndDelete(key, value, 0); err != nil {
						m.ctx.Log(metafora.LogLevelWarn, "Error releasing task %s while stopping: %v", taskID, err)
					}
				}
				return
			}
		}
	}()
	return true
}

// remove tells a single task's refresher to stop.
func (m *taskManager) remove(taskID string, done bool) {
	m.taskL.Lock()
	defer m.taskL.Unlock()
	stop, ok := m.tasks[taskID]
	if !ok {
		m.ctx.Log(metafora.LogLevelDebug, "Cannot remove task %s from refresher: not present.", taskID)
		return
	}

	select {
	case stop <- taskExit{done: done}:
	default:
		m.ctx.Log(metafora.LogLevelDebug, "Pending stop signal for task %s", taskID)
	}
}

// stop blocks until all refreshers have exited.
func (m *taskManager) stop() {
	func() {
		m.taskL.Lock()
		defer m.taskL.Unlock()
		for task, stop := range m.tasks {
			select {
			case stop <- taskExit{done: false}:
			default:
				m.ctx.Log(metafora.LogLevelDebug, "Pending stop signal for task %s", task)
			}
		}
	}()
	m.wg.Wait()
}
