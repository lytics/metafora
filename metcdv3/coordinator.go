package metcdv3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

type ownerValue struct {
	Node string `json:"node"`
}

type EtcdV3Coordinator struct {
	cordCtx metafora.CoordinatorContext
	conf    *Config
	name    string

	commandPath string
	nodePath    string
	taskPath    string

	newTask TaskFunc

	closeOnce *sync.Once

	done          chan bool
	exited        chan bool
	etcdv3c       *etcdv3.Client
	kvc           etcdv3.KV
	leaseID       etcdv3.LeaseID
	lease         etcdv3.Lease
	LeaseDuration time.Duration
	Timeout       time.Duration
	// Testing hook.
	keepAliveStats *keepAliveStats
}

func New(conf *Config, etcdv3c *etcdv3.Client, h statemachine.StatefulHandler) (metafora.Coordinator, metafora.HandlerFunc, metafora.Balancer) {
	ss := NewStateStore(conf.Namespace, etcdv3c)

	// Create a HandlerFunc that ties together the command listener, stateful
	// handler, and statemachine.
	hf := func(task metafora.Task) metafora.Handler {
		cl := NewCommandListener(conf, task, etcdv3c)
		return statemachine.New(task, h, ss, cl, nil)
	}

	// Create an etcd coordinator
	coord := NewEtcdV3Coordinator(conf, etcdv3c)

	filter := func(_ *FilterableValue) bool { return true }
	// Create an etcd backed Fair Balancer (there's no harm in not using it)
	bal := NewFairBalancer(conf, etcdv3c, filter)
	return coord, hf, bal
}

func NewEtcdV3Coordinator(conf *Config, client *etcdv3.Client) *EtcdV3Coordinator {
	nodePath := path.Join(conf.Namespace, NodesPath, conf.Name)
	// Protect callers of task functions from panics.
	newTask := func(id, value string) metafora.Task {
		// TODO Is this necessary anymore?
		defer func() {
			if p := recover(); p != nil {
				metafora.Errorf("%s panic when creating task: %v", conf.String(), p)
			}
		}()
		return conf.NewTaskFunc(id, value)
	}
	return &EtcdV3Coordinator{
		closeOnce:     &sync.Once{},
		done:          make(chan bool),
		exited:        make(chan bool),
		etcdv3c:       client,
		kvc:           etcdv3.NewKV(client),
		leaseID:       -1,
		name:          conf.String(),
		conf:          conf,
		Timeout:       10 * time.Second,
		LeaseDuration: 60 * time.Second,

		nodePath:    nodePath,
		commandPath: path.Join(nodePath, CommandsPath),
		taskPath:    path.Join(conf.Namespace, TasksPath),

		newTask: newTask,
	}

}

// Watch streams tasks from etcd watches or GETs until Close is called or etcd
// is unreachable (in which case an error is returned).
func (ec *EtcdV3Coordinator) Watch(out chan<- metafora.Task) error {
	c := context.Background()
	select {
	case <-ec.done:
		return nil
	default:
	}

	parseTask := func(we *WatchEvent) metafora.Task {
		// Pickup new tasks
		if we.Type != Delete {
			dir, taskID := path.Split(we.Key)
			dir = path.Clean(dir)
			if taskID == PropsPath {
				_, taskID := path.Split(dir)
				metafora.Debugf("%s received task with properties: %s", ec.name, taskID)
				if resp, err := ec.kvc.Get(c, path.Join(dir, OwnerPath), etcdv3.WithLimit(1)); err != nil {
					metafora.Errorf("%s error getting properties while handling %s", ec.name, taskID)
					return nil
				} else if resp.Count == 1 {
					metafora.Debugf("%s received claimed task: %s", ec.name, taskID)
					return nil
				}
				return ec.newTask(taskID, string(we.Value))
			}
			if dir == ec.taskPath {
				// Sadly we need to fail parsing this task if there's an error getting the
				// props file as trying to claim a task without properly knowing its
				// properties could cause major issues.
				metafora.Debugf("%s received new task: %s", ec.name, taskID)
				resp, err := ec.kvc.Get(c, path.Join(we.Key, PropsPath), etcdv3.WithLimit(1))
				if err != nil {
					metafora.Errorf("%s error getting properties while handling %s", ec.name, taskID)
					return nil
				}
				props := ""
				if resp.Count > 0 {
					props = string(resp.Kvs[0].Value)
				}
				if resp, err = ec.kvc.Get(c, path.Join(we.Key, OwnerPath), etcdv3.WithLimit(1)); err != nil {
					metafora.Errorf("%s error getting properties while handling %s", ec.name, taskID)
					return nil
				} else if resp.Count == 1 {
					metafora.Debugf("%s received claimed task: %s", ec.name, taskID)
					return nil
				}
				return ec.newTask(taskID, props)
			}
		} else {
			dir, end := path.Split(we.Key)
			dir = path.Clean(dir)
			// If a claim key is removed, try to claim the task
			if end == OwnerPath {
				taskID := path.Base(dir)
				metafora.Debugf("%s received released task: %s", ec.name, taskID)

				// Sadly we need to fail parsing this task if there's an error getting the
				// props file as trying to claim a task without properly knowing its
				// properties could cause major issues.
				resp, err := ec.kvc.Get(c, path.Join(dir, PropsPath), etcdv3.WithLimit(1))
				if err != nil {
					metafora.Errorf("%s error getting properties while handling %s", ec.name, taskID)
					return nil
				}
				props := ""
				if resp.Count > 0 {
					props = string(resp.Kvs[0].Value)
				}
				return ec.newTask(taskID, props)
			}
		}

		// Ignore any other key events (_metafora keys, task deletion, etc.)
		return nil
	}

	getRes, err := ec.kvc.Get(c, ec.taskPath, etcdv3.WithPrefix())
	if err != nil {
		metafora.Errorf("metafora etcdv3 coordinator: Error GETting %s - sending error to stateful handler: %v", ec.taskPath, err)
		return err
	}

	for _, kv := range getRes.Kvs {
		key := string(kv.Key)
		if base := path.Base(key); base == OwnerPath || base == MetadataPath || base == PropsPath {
			continue
		}

		we := &WatchEvent{
			Key:   key,
			Value: kv.Value,
		}
		task := parseTask(we)
		if task != nil {
			select {
			case <-c.Done():
				return nil
			case <-ec.done:
				return nil
			case out <- task:
			}
		}
	}

	watchEvents, err := ec.watch(c, ec.taskPath, getRes.Header.Revision)
	if err != nil {
		return err
	}

	var watchEvent *WatchEvent
	for {
		select {
		case <-c.Done():
			return nil
		case <-ec.done:
			return nil
		case watchEvent = <-watchEvents:
			if watchEvent.Error != nil {
				return watchEvent.Error
			}

			key := string(watchEvent.Key)
			if base := path.Base(key); base == MetadataPath || base == PropsPath {
				continue
			}

			task := parseTask(watchEvent)
			if task != nil {
				select {
				case <-c.Done():
					return nil
				case <-ec.done:
					return nil
				case out <- task:
				}
			}
		}
	}
}

func (ec *EtcdV3Coordinator) ownerNode(taskID string) (key, value string) {
	p, err := json.Marshal(&ownerValue{Node: ec.conf.Name})
	if err != nil {
		panic(fmt.Sprintf("coordinator: error marshalling node body: %v", err))
	}
	return path.Join(ec.taskPath, taskID, OwnerPath), string(p)
}

func (ec *EtcdV3Coordinator) Claim(task metafora.Task) bool {
	c := context.Background()
	tid := task.ID()
	// Attempt to claim the node
	key, value := ec.ownerNode(tid)
	txnRes, err := ec.kvc.Txn(c).
		If(etcdv3.Compare(etcdv3.Version(key), "=", 0), etcdv3.Compare(etcdv3.Version(path.Join(ec.taskPath, tid)), ">", 0)).
		Then(etcdv3.OpPut(key, value, etcdv3.WithLease(ec.leaseID))).
		Commit()
	if err != nil {
		metafora.Errorf("Claim of %s failed with an unexpected error: %v", key, err)
		return false
	}
	if !txnRes.Succeeded {
		metafora.Debugf("Claim of %s failed, already claimed", key)
		return false
	}

	// Claim successful, start the refresher
	metafora.Debugf("Claim successful: %s", key)
	return true
}

func (ec *EtcdV3Coordinator) Release(task metafora.Task) {
	c := context.Background()
	tid := task.ID()
	key, value := ec.ownerNode(tid)
	metafora.Debugf("Deleting claim for task %s as it's released.", tid)
	txnRes, err := ec.kvc.Txn(c).
		If(etcdv3.Compare(etcdv3.Value(key), "=", value)).
		Then(etcdv3.OpDelete(key, etcdv3.WithPrefix())).
		Commit()
	if err != nil {
		metafora.Warnf("Error releasing task %s while stopping: %v", tid, err)
	}
	if txnRes == nil || !txnRes.Succeeded {
		metafora.Warnf("Failed to release task %s while stopping.", tid)
	}
}

func (ec *EtcdV3Coordinator) Done(task metafora.Task) {
	tid := task.ID()
	metafora.Debugf("Deleting directory for task %s as it's done.", tid)
	if _, err := ec.kvc.Delete(context.Background(), path.Join(ec.taskPath, tid), etcdv3.WithPrefix()); err != nil {
		metafora.Errorf("Error deleting task %s while stopping: %v", tid, err)
	}
}

func (ec *EtcdV3Coordinator) Command() (metafora.Command, error) {
	c := context.Background()
	select {
	case <-ec.done:
		return nil, nil
	default:
	}

	getRes, err := ec.kvc.Get(c, ec.commandPath, etcdv3.WithPrefix())
	if err != nil {
		metafora.Errorf("metafora etcdv3 coordinator: Error GETting %s - sending error to stateful handler: %v", ec.commandPath, err)
		return nil, err
	}

	parseCommand := func(we *WatchEvent) (metafora.Command, error) {
		if _, err := ec.kvc.Delete(c, we.Key, etcdv3.WithPrefix()); err != nil {
			metafora.Errorf("metafora etcdv3 coordinator: Error deleting handled command %s: %v", we.Key, err)
			return nil, err
		}

		cmd, err := metafora.UnmarshalCommand(we.Value)
		if err != nil {
			metafora.Errorf("Invalid command %s: %v", we.Key, err)
		}
		return cmd, err
	}

	for _, kv := range getRes.Kvs {
		key := string(kv.Key)
		if path.Base(key) == MetadataPath || key == ec.commandPath {
			continue
		}

		we := &WatchEvent{
			Key:   key,
			Value: kv.Value,
		}
		return parseCommand(we)
	}

	watchEvents, err := ec.watch(c, ec.commandPath, getRes.Header.Revision)
	if err != nil {
		return nil, err
	}

	var watchEvent *WatchEvent
	for {
		select {
		case <-c.Done():
			return nil, nil
		case <-ec.done:
			return nil, nil
		case watchEvent = <-watchEvents:
			if watchEvent.Error != nil {
				return nil, watchEvent.Error
			}

			if path.Base(watchEvent.Key) == MetadataPath || watchEvent.Key == ec.commandPath {
				continue
			}

			return parseCommand(watchEvent)
		}
	}
}

func (ec *EtcdV3Coordinator) addMetadata(basepath string) error {
	//hidden etcd key that isn't visible to ls commands on the directory,
	//  you have to know about it to find it :).  I'm using it to add some
	//  info about when the cluster's schema was setup.
	metaPath := path.Join(basepath, MetadataPath)
	host, err := os.Hostname()

	metadataStruct := struct {
		Host        string `json:"host"`
		CreatedTime string `json:"created"`
		ownerValue
	}{
		Host:        host,
		CreatedTime: time.Now().String(),
		ownerValue:  ownerValue{Node: ec.conf.Name},
	}
	metadata, err := json.Marshal(metadataStruct)
	if err != nil {
		return err
	}

	_, err = ec.kvc.Put(context.Background(), metaPath, string(metadata), etcdv3.WithLease(ec.leaseID))
	return err
}

func (ec *EtcdV3Coordinator) Name() string {
	return ec.name
}

// Init starts an mclient.
func (ec *EtcdV3Coordinator) Init(cordCtx metafora.CoordinatorContext) error {
	ec.cordCtx = cordCtx

	if ec.LeaseDuration < minLeaseDuration {
		return ErrLeaseDurationTooShort
	}
	ec.lease = etcdv3.NewLease(ec.etcdv3c)

	timeout, cancel := context.WithTimeout(context.Background(), ec.Timeout)
	res, err := ec.lease.Grant(timeout, int64(ec.LeaseDuration.Seconds()))
	cancel()
	if err != nil {
		return err
	}
	ec.leaseID = res.ID

	// Start the keep alive for the lease.
	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	keepAlive, err := ec.lease.KeepAlive(keepAliveCtx, ec.leaseID)
	if err != nil {
		keepAliveCancel()
		return err
	}

	// There are two ways the Client can exit:
	//     1) Someone calls Stop, in which case it will cancel
	//        its context and exit.
	//     2) The Client fails to signal keep-alive on it
	//        lease repeatedly, in which case it will cancel
	//        its context and exit.
	go func() {
		defer close(ec.exited)

		// Track stats related to keep alive responses.
		stats := &keepAliveStats{}
		defer func() {
			ec.keepAliveStats = stats
		}()

		for {
			select {
			case <-ec.done:
				keepAliveCancel()
			case res, open := <-keepAlive:
				if !open {
					// When the keep alive closes, check
					// if this was a close requested by
					// the user of the registry, or if
					// it was unexpected. If it was by
					// the user, the 'done' channel should
					// be closed.
					select {
					case <-ec.done:
						metafora.Infof("metafora etcdv3 coordinator: %v: keep alive closed", ec.name)
						return
					default:
					}
					// Testing hook.
					if stats != nil {
						stats.failure++
					}
					panic(fmt.Sprintf("metafora etcdv3 coordinator: %v: keep alive closed unexpectedly", ec.name))
					return
				}
				metafora.Infof("metafora etcdv3 coordinator: %v: keep alive responded with heartbeat TTL: %vs", ec.name, res.TTL)
				// Testing hook.
				if stats != nil {
					stats.success++
				}
			}
		}
	}()

	err = ec.addMetadata(ec.conf.Namespace)
	if err != nil {
		return err
	}
	err = ec.addMetadata(ec.nodePath)
	if err != nil {
		return err
	}
	err = ec.addMetadata(ec.taskPath)
	if err != nil {
		return err
	}
	err = ec.addMetadata(ec.commandPath)
	if err != nil {
		return err
	}

	return nil
}

// Close EtcdV3Coordinator.
func (ec *EtcdV3Coordinator) Close() {
	if ec.leaseID < 0 {
		return
	}
	ec.closeOnce.Do(func() {
		// Close the done channel, to indicate
		// that this registry is done to its
		// background go-routines, such as the
		// keep-alive go-routine.
		close(ec.done)
		// Wait for those background go-routines
		// to actually exit.
		<-ec.exited
		// Then revoke the lease to cleanly remove
		// all keys associated with this registry
		// from etcd.
		timeout, cancel := context.WithTimeout(context.Background(), ec.Timeout)
		_, err := ec.lease.Revoke(timeout, ec.leaseID)
		cancel()
		if err != nil {
			metafora.Errorf("metaphora etcdv3 coordinator: error while closing lease: %v", err)
		}
	})
}

// EventType of a watch event.
type EventType int

const (
	Error  EventType = 0
	Delete EventType = 1
	Modify EventType = 2
	Create EventType = 3
)

// WatchEvent triggred by a change in the registry.
type WatchEvent struct {
	Key   string
	Value []byte
	Type  EventType
	Error error
}

// String representation of the watch event.
func (we *WatchEvent) String() string {
	if we.Error != nil {
		return fmt.Sprintf("key: %v, error: %v", we.Key, we.Error)
	}
	typ := "delete"
	switch we.Type {
	case Modify:
		typ = "modify"
	case Create:
		typ = "create"
	}
	return fmt.Sprintf("key: %v, type: %v, value: %v", we.Key, typ, we.Value)
}

// Watch a prefix in the coordinator.
func (ec *EtcdV3Coordinator) watch(c context.Context, prefix string, revision int64) (<-chan *WatchEvent, error) {
	// Channel to publish registry changes.
	watchEvents := make(chan *WatchEvent)

	// Write a change or exit the watcher.
	put := func(we *WatchEvent) {
		select {
		case <-c.Done():
			return
		case <-ec.done:
			return
		case watchEvents <- we:
		}
	}
	putTerminalError := func(we *WatchEvent) {
		select {
		case <-c.Done():
		case <-time.After(10 * time.Minute):
		case watchEvents <- we:
		}
	}
	// Create a watch-event from an event.
	createWatchEvent := func(ev *etcdv3.Event) *WatchEvent {
		wev := &WatchEvent{Key: string(ev.Kv.Key)}
		if ev.IsCreate() {
			wev.Type = Create
		} else if ev.IsModify() {
			wev.Type = Modify
		} else {
			wev.Type = Delete
			// Need to return now because
			// delete events don't contain
			// any data to unmarshal.
			return wev
		}
		wev.Value = ev.Kv.Value
		return wev
	}

	// Watch deltas in etcd, with the give prefix, starting
	// at the revision of the get call above.
	deltas := ec.etcdv3c.Watch(c, prefix, etcdv3.WithPrefix(), etcdv3.WithRev(revision+1))
	go func() {
		defer close(watchEvents)
		for {
			select {
			case delta, open := <-deltas:
				if !open {
					putTerminalError(&WatchEvent{Error: ErrWatchClosedUnexpectedly})
					return
				}
				if delta.Err() != nil {
					putTerminalError(&WatchEvent{Error: delta.Err()})
					return
				}
				for _, event := range delta.Events {
					put(createWatchEvent(event))
				}
			}
		}
	}()

	return watchEvents, nil
}
