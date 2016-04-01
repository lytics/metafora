package m_etcd

import (
	"path"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
	"golang.org/x/net/context"
)

const (
	// etcd/Response.Action values
	actionCreated = "create"
	actionSet     = "set"
	actionExpire  = "expire"
	actionDelete  = "delete"
	actionCAD     = "compareAndDelete"
)

var (
	createOrdered    = &client.CreateInOrderOptions{}
	delRecur         = &client.DeleteOptions{Dir: false, Recursive: true}
	delRecurDir      = &client.DeleteOptions{Dir: true, Recursive: true}
	delOne           = &client.DeleteOptions{Dir: false, Recursive: false}
	getNoSortNoRecur = &client.GetOptions{Recursive: false, Sort: false, Quorum: true}
	getNoSortRecur   = &client.GetOptions{Recursive: true, Sort: false, Quorum: true}
	getSortRecur     = &client.GetOptions{Recursive: true, Sort: true, Quorum: true}
	setCreateDir     = &client.SetOptions{Dir: true}
	setDefault       = &client.SetOptions{}
)

var (
	// etcd actions signifying a claim key was released
	releaseActions = map[string]bool{
		actionExpire: true,
		actionDelete: true,
		actionCAD:    true,
	}

	// etcd actions signifying a new key
	newActions = map[string]bool{
		actionCreated: true,
		actionSet:     true,
	}
)

type ownerValue struct {
	Node string `json:"node"`
}

type EtcdCoordinator struct {
	client  client.KeysAPI
	cordCtx metafora.CoordinatorContext
	conf    *Config
	name    string

	commandPath string
	nodePath    string
	taskPath    string

	newTask     TaskFunc
	taskManager *taskManager

	// Close() closes stop channel to signal to watchers to exit
	stop chan bool
}

func (ec *EtcdCoordinator) closed() bool {
	select {
	case <-ec.stop:
		return true
	default:
		return false
	}
}

// New creates a Metafora Coordinator, State Machine, State Store, Fair
// Balancer, and Commander, all backed by etcd.
//
// Create a Config and implement your task handler as a StatefulHandler. Then
// New will create all the components needed to call metafora.NewConsumer:
//
//  conf := m_etcd.NewConfig("work", hosts)
//	coord, hf, bal, err := m_etcd.New(conf, customHandler)
//	if err != nil { /* ...exit... */ }
//	consumer, err := metafora.NewConsumer(coord, hf, bal)
//
func New(conf *Config, h statemachine.StatefulHandler) (
	metafora.Coordinator, metafora.HandlerFunc, metafora.Balancer, error) {

	kc, err := newEtcdClient(conf.EtcdConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create the state store
	ss := NewStateStore(conf)

	// Create a HandlerFunc that ties together the command listener, stateful
	// handler, and statemachine.
	hf := func(task metafora.Task) metafora.Handler {
		cl := NewCommandListener(task, conf.Namespace, kc)
		return statemachine.New(task, h, ss, cl, nil)
	}

	// Create an etcd coordinator
	coord, err := NewEtcdCoordinator(conf)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create an etcd backed Fair Balancer (there's no harm in not using it)
	bal := NewFairBalancer(conf, kc)

	return coord, hf, bal, nil
}

// NewEtcdCoordinator creates a new Metafora Coordinator implementation using
// etcd as the broker. If no node ID is specified, a unique one will be
// generated.
//
// Coordinator methods will be called by the core Metafora Consumer. Calling
// Init, Close, etc. from your own code will lead to undefined behavior.
func NewEtcdCoordinator(conf *Config) (*EtcdCoordinator, error) {
	kc, err := newEtcdClient(conf.EtcdConfig)
	if err != nil {
		return nil, err
	}

	ec := &EtcdCoordinator{
		client: kc,
		conf:   conf,
		name:   conf.String(),

		commandPath: path.Join(conf.Namespace, NodesPath, conf.Name, CommandsPath),
		nodePath:    path.Join(conf.Namespace, NodesPath, conf.Name),
		taskPath:    path.Join(conf.Namespace, TasksPath),

		stop: make(chan bool),
	}

	// Protect callers of task functions from panics.
	ec.newTask = func(id, value string) metafora.Task {
		defer func() {
			if p := recover(); p != nil {
				metafora.Errorf("%s panic when creating task: %v", ec.name, p)
			}
		}()
		return conf.NewTaskFunc(id, value)
	}

	return ec, nil
}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) error {
	metafora.Debugf("Initializing coordinator with namespace: %s", ec.conf.Namespace)

	ec.cordCtx = cordCtx

	ec.client.Set(context.TODO(), ec.conf.Namespace, "", setCreateDir)
	ec.client.Set(context.TODO(), ec.taskPath, "", setCreateDir)

	nodePathOpts := &client.SetOptions{TTL: ec.conf.NodeTTL, Dir: true}
	if _, err := ec.client.Set(context.TODO(), ec.nodePath, "", nodePathOpts); err != nil {
		return err
	}

	// Start goroutine to heartbeat node key in etcd
	go ec.nodeRefresher()
	ec.client.Set(context.TODO(), ec.commandPath, "", setCreateDir)

	ec.taskManager = newManager(cordCtx, ec.client, ec.taskPath, ec.conf.Name, ec.conf.ClaimTTL)
	return nil
}

// nodeRefresher is in charge of keeping the node entry in etcd alive. If it's
// unable to communicate with etcd it must shutdown the coordinator.
//
// watch retries on errors and taskmgr calls Lost(task) on tasks it can't
// refresh, so it's up to nodeRefresher to cause the coordinator to close if
// it's unable to communicate with etcd.
func (ec *EtcdCoordinator) nodeRefresher() {
	ttl := ec.conf.NodeTTL >> 1 // have some leeway before ttl expires
	if ttl < 1 {
		metafora.Warnf("%s Dangerously low NodeTTL: %d", ec.name, ec.conf.NodeTTL)
		ttl = 1
	}

	for {
		// Deadline for refreshes to finish by or the coordinator closes.
		deadline := time.Now().Add(time.Duration(ec.conf.NodeTTL) * time.Second)
		select {
		case <-ec.stop:
			return
		case <-time.After(time.Duration(ttl) * time.Second):
			if err := ec.refreshBy(deadline); err != nil {
				// We're in a bad state; shut everything down
				metafora.Errorf("Unable to refresh node key before deadline %s. Last error: %v", deadline, err)
				ec.Close()
			}
		}
	}
}

// refreshBy retries refreshing the node key until the deadline is reached.
func (ec *EtcdCoordinator) refreshBy(deadline time.Time) (err error) {
	for time.Now().Before(deadline) {
		// Make sure we shouldn't exit
		select {
		case <-ec.stop:
			return err
		default:
		}

		ops := &client.SetOptions{Dir: true, Refresh: true, TTL: ec.conf.NodeTTL}
		ctx, cancel := context.WithDeadline(context.TODO(), deadline)
		_, err = ec.client.Set(ctx, ec.nodePath, "", ops)
		cancel()
		if err == nil {
			// It worked, stop retrying
			return nil
		}
		metafora.Warnf("Unexpected error updating node key, retrying until %s: %v", deadline, err)
		time.Sleep(500 * time.Millisecond) // rate limit retries a bit
	}
	// Didn't get a successful response before deadline, exit with error
	return err
}

// Watch streams tasks from etcd watches or GETs until Close is called or etcd
// is unreachable (in which case an error is returned).
func (ec *EtcdCoordinator) Watch(out chan<- metafora.Task) error {
	opts := &client.WatcherOptions{Recursive: true}

startWatch:
	for {
		// Make sure we haven't been told to exit
		select {
		case <-ec.stop:
			return nil
		default:
		}

		// Get existing tasks
		resp, err := ec.client.Get(context.TODO(), ec.taskPath, getNoSortRecur)
		if err != nil {
			metafora.Errorf("%s Error getting the existing tasks: %v", ec.taskPath, err)
			return err
		}

		// Start watching at the index the Get retrieved since we've retrieved all
		// tasks up to that point.
		opts.AfterIndex = resp.Index

		// Act like existing keys are newly created
		for _, node := range resp.Node.Nodes {
			if task := ec.parseTask(&client.Response{Action: "create", Node: node}); task != nil {
				select {
				case out <- task:
				case <-ec.stop:
					return nil
				}
			}
		}

		// Start blocking watch
		watcher := ec.client.Watcher(ec.taskPath, opts)
		for {
			//TODO remove once Watcher's context checks ec.stop
			select {
			case <-ec.stop:
				return nil
			default:
			}

			//TODO use ec.stop in context
			resp, err := watcher.Next(context.TODO())
			if err != nil {
				if ee, ok := err.(*client.Error); ok && ee.Code == client.ErrorCodeEventIndexCleared {
					// Need to restart watch with a new Get
					continue startWatch
				}

				//FIXME what error is this replaced by?!
				if err.Error() == "ErrWatchStoppedByUser" {
					return nil
				}
				return err
			}

			// Found a claimable task!
			if task := ec.parseTask(resp); task != nil {
				select {
				case out <- task:
				case <-ec.stop:
					return nil
				}
			}
		}
	}
}

func (ec *EtcdCoordinator) parseTask(resp *client.Response) metafora.Task {
	// Sanity check / test path invariant
	if !strings.HasPrefix(resp.Node.Key, ec.taskPath) {
		metafora.Errorf("%s received task from outside task path: %s", ec.name, resp.Node.Key)
		return nil
	}

	key := strings.Trim(resp.Node.Key, "/") // strip leading and trailing /s
	parts := strings.Split(key, "/")

	// Pickup new tasks
	if newActions[resp.Action] && len(parts) == 3 && resp.Node.Dir {
		// Make sure it's not already claimed before returning it
		for _, n := range resp.Node.Nodes {
			if strings.HasSuffix(n.Key, OwnerMarker) {
				metafora.Debugf("%s ignoring task as it's already claimed: %s", ec.name, parts[2])
				return nil
			}
		}
		metafora.Debugf("%s received new task: %s", ec.name, parts[2])
		props := ""
		for _, n := range resp.Node.Nodes {
			if strings.HasSuffix(n.Key, "/"+PropsKey) {
				props = n.Value
				break
			}
		}
		return ec.newTask(parts[2], props)
	}

	if newActions[resp.Action] && len(parts) == 4 && parts[3] == PropsKey {
		metafora.Debugf("%s received task with properties: %s", ec.name, parts[2])
		return ec.newTask(parts[2], resp.Node.Value)
	}

	// If a claim key is removed, try to claim the task
	if releaseActions[resp.Action] && len(parts) == 4 && parts[3] == OwnerMarker {
		metafora.Debugf("%s received released task: %s", ec.name, parts[2])

		// Sadly we need to fail parsing this task if there's an error getting the
		// props file as trying to claim a task without properly knowing its
		// properties could cause major issues.
		parts[3] = PropsKey
		propsnode, err := ec.client.Get(context.TODO(), path.Join(parts...), getNoSortNoRecur)
		if err != nil {
			if ee, ok := err.(*client.Error); ok && ee.Code == client.ErrorCodeKeyNotFound {
				// No props file
				return ec.newTask(parts[2], "")
			}

			metafora.Errorf("%s error getting properties while handling %s", ec.name, parts[2])
			return nil
		}
		return ec.newTask(parts[2], propsnode.Node.Value)
	}

	// Ignore any other key events (_metafora keys, task deletion, etc.)
	return nil
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if the task could not be claimed.
// Either due to error, the task being completed, or another consumer has
// already claimed it.
func (ec *EtcdCoordinator) Claim(task metafora.Task) bool {
	return ec.taskManager.add(task)
}

// Release deletes the claim file.
func (ec *EtcdCoordinator) Release(task metafora.Task) {
	const done = false
	ec.taskManager.remove(task.ID(), done)
}

// Done deletes the task.
func (ec *EtcdCoordinator) Done(task metafora.Task) {
	const done = true
	ec.taskManager.remove(task.ID(), done)
}

// Command blocks until a command for this node is received from the broker
// by the coordinator.
func (ec *EtcdCoordinator) Command() (metafora.Command, error) {
	if ec.closed() {
		// already closed, don't restart watch
		return nil, nil
	}

	opts := &client.WatcherOptions{Recursive: true}

startWatch:
	for {
		// Get existing commands
		resp, err := ec.client.Get(context.TODO(), ec.commandPath, getSortRecur)
		if err != nil {
			metafora.Errorf("%s Error getting the existing commands: %v", ec.commandPath, err)
			return nil, err
		}

		// Start watching at the index the Get retrieved since we've retrieved all
		// tasks up to that point.
		opts.AfterIndex = resp.Index

		// Act like existing keys are newly created
		for _, node := range resp.Node.Nodes {
			if cmd := ec.parseCommand(&client.Response{Action: "create", Node: node}); cmd != nil {
				return cmd, nil
			}
		}

		//TODO use ec.stop in context
		watcher := ec.client.Watcher(ec.taskPath, opts)
		for {
			//TODO remove once Watcher's context checks ec.stop
			select {
			case <-ec.stop:
				return nil, nil
			default:
			}

			resp, err := watcher.Next(context.TODO())
			if err != nil {
				if ee, ok := err.(*client.Error); ok && ee.Code == client.ErrorCodeEventIndexCleared {
					// Need to restart watch with a new Get
					continue startWatch
				}

				//FIXME what to replace this with?!
				if err.Error() == "ErrWatchStoppedByUser" {
					return nil, nil
				}
				return nil, err
			}

			if cmd := ec.parseCommand(resp); cmd != nil {
				return cmd, nil
			}
		}
	}
}

func (ec *EtcdCoordinator) parseCommand(resp *client.Response) metafora.Command {
	if strings.HasSuffix(resp.Node.Key, MetadataKey) {
		// Skip metadata marker
		return nil
	}

	if _, err := ec.client.Delete(context.TODO(), resp.Node.Key, delOne); err != nil {
		metafora.Errorf("Error deleting handled command %s: %v", resp.Node.Key, err)
	}

	cmd, err := metafora.UnmarshalCommand([]byte(resp.Node.Value))
	if err != nil {
		metafora.Errorf("Invalid command %s: %v", resp.Node.Key, err)
		return nil
	}
	return cmd
}

// Close stops the coordinator and causes blocking Watch and Command methods to
// return zero values. It does not release tasks.
func (ec *EtcdCoordinator) Close() {
	// Gracefully handle multiple close calls mostly to ease testing. This block
	// isn't threadsafe, so you shouldn't try to call Close() concurrently.
	select {
	case <-ec.stop:
		return
	default:
	}

	close(ec.stop)

	// Finally remove the node entry
	_, err := ec.client.Delete(context.TODO(), ec.nodePath, delRecur)
	if err != nil {
		if client.IsKeyNotFound(err) {
			// The node's TTL was up before we were able to delete it or there was
			// another problem that's already being handled.  The first is unlikely,
			// the latter is already being handled, so there's nothing to do here.
			return
		}
		// All other errors are unexpected
		metafora.Errorf("Error deleting node path %s: %v", ec.nodePath, err)
	}
}

func (ec *EtcdCoordinator) Name() string {
	return ec.name
}

func newEtcdClient(conf client.Config) (client.KeysAPI, error) {
	c, err := client.New(conf)
	if err != nil {
		metafora.Errorf("Error creating etcd client: %v", err)
		return nil, err
	}
	if err := c.Sync(context.TODO()); err != nil {
		metafora.Errorf("Error syncing with etcd cluster: %v", err)
		return nil, err
	}
	return client.NewKeysAPI(c), nil
}
