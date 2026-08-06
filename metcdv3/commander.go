package metcdv3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrWatchClosedUnexpectedly = errors.New("metafora: watch closed unexpectedly")
	// ErrWatchCompacted is reported when etcd has compacted away the revision
	// the watch was started from (e.g. around member restarts/relocations).
	// Callers should re-sync with a fresh Get and restart the watch rather
	// than treating it as fatal.
	ErrWatchCompacted = errors.New("metafora: watch revision compacted")
)

type cmdr struct {
	etcdv3c   *etcdv3.Client
	kvc       etcdv3.KV
	taskspath string
}

func NewCommander(namespace string, c *etcdv3.Client) statemachine.Commander {
	return &cmdr{
		taskspath: path.Join("/", namespace, TasksPath),
		etcdv3c:   c,
		kvc:       etcdv3.NewKV(c),
	}
}

// Send command to a task. Overwrites existing commands.
func (c *cmdr) Send(taskID string, m *statemachine.Message) error {
	buf, err := json.Marshal(m)
	if err != nil {
		return err
	}

	cmdPath := path.Join(c.taskspath, taskID, CommandsPath)
	_, err = c.kvc.Put(context.Background(), cmdPath, string(buf))
	return err
}

type cmdListener struct {
	etcdv3c       *etcdv3.Client
	kvc           etcdv3.KV
	name          string
	taskid        string
	taskcmdpath   string
	taskownerpath string

	commands chan *statemachine.Message

	mu     *sync.Mutex
	stop   chan bool
	cancel context.CancelFunc
}

// NewCommandListener makes a statemachine.CommandListener implementation
// backed by etcd. The namespace should be the same as the coordinator as
// commands use a separate path within a namespace than tasks or nodes.
func NewCommandListener(conf *Config, task metafora.Task, c *etcdv3.Client) statemachine.CommandListener {
	taskpath := path.Join("/", conf.Namespace, TasksPath, task.ID())

	ctxt, cancel := context.WithCancel(context.Background())
	cl := &cmdListener{
		etcdv3c:       c,
		name:          conf.Name,
		taskid:        task.ID(),
		taskcmdpath:   path.Join(taskpath, CommandsPath),
		taskownerpath: path.Join(taskpath, OwnerPath),
		kvc:           etcdv3.NewKV(c),
		commands:      make(chan *statemachine.Message),
		mu:            &sync.Mutex{},
		stop:          make(chan bool),
		cancel:        cancel,
	}
	go cl.watch(ctxt, cl.taskcmdpath)
	return cl
}

func (c *cmdListener) Receive() <-chan *statemachine.Message {
	return c.commands
}

func (c *cmdListener) ownerValueString() string {
	p, err := json.Marshal(&ownerValue{Node: c.name})
	if err != nil {
		panic(fmt.Sprintf("command listener: error marshalling node body: %v", err))
	}
	return string(p)
}

func (c *cmdListener) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cancel()
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
}

func (cl *cmdListener) watch(c context.Context, prefix string) {
	putTerminalError := func(msg *statemachine.Message) {
		go func() {
			select {
			case <-c.Done():
				// TODO Do I need the stop channel?
			case <-cl.stop:
			case <-time.After(10 * time.Minute):
				metafora.Warnf("metafora command listener timed out putting message on channel: %v", msg)
			case cl.commands <- msg:
			}
		}()
	}

	var backoff resyncBackoff
	for {
		start := time.Now()
		err := cl.watchOnce(c, prefix)
		if err == nil {
			return // shutdown
		}
		if !errors.Is(err, ErrWatchCompacted) {
			putTerminalError(statemachine.ErrorMessage(err))
			return
		}

		delay, escalated := backoff.next(time.Since(start))
		if escalated {
			// Re-syncs are failing in a tight loop; fall back to surfacing the
			// error (faulting the task) rather than re-syncing forever.
			metafora.Errorf("metafora command listener: watch %s compacted %d times in a row; giving up and faulting task", prefix, backoff.consecutive)
			putTerminalError(statemachine.ErrorMessage(err))
			return
		}
		metafora.Warnf("metafora command listener: watch revision compacted, re-syncing %s in %s", prefix, delay)
		select {
		case <-c.Done():
			return
		case <-cl.stop:
			return
		case <-time.After(delay):
		}
	}
}

// watchOnce performs a single GET-then-watch episode for a task's commands,
// emitting command messages as they arrive. It returns ErrWatchCompacted if
// etcd compacted the watch revision (so watch can re-sync), nil on shutdown, or
// the underlying error otherwise (the caller faults the task with it).
func (cl *cmdListener) watchOnce(c context.Context, prefix string) error {
	// Create a message from an event.
	createMessage := func(key string, value []byte) (*statemachine.Message, error) {
		msg := &statemachine.Message{}
		if err := json.Unmarshal(value, msg); err != nil {
			metafora.Errorf("Error unmarshalling command from %s - sending error to stateful handler: %v", key, err)
			return nil, err
		}

		// Compare against the owner key built from the task path. Deriving it
		// from the command key's parent instead silently resolves to the wrong
		// key if the command ever moves within the prefix being watched.
		txnRes, err := cl.kvc.Txn(c).
			If(etcdv3.Compare(etcdv3.Value(cl.taskownerpath), "=", cl.ownerValueString())).
			Then(etcdv3.OpDelete(key)).
			Commit()
		if err != nil {
			metafora.Errorf("Error deleting command %s: %s - sending error to stateful handler: %v", key, msg, err)
			return nil, err
		}
		if !txnRes.Succeeded {
			// This node no longer owns the task, so it must not consume the
			// command; whoever holds the claim will pick it up instead.
			metafora.Warnf("task=%q no longer owned by %s, leaving command %s at %s for the new owner", cl.taskid, cl.name, msg, key)
			return nil, nil
		}
		return msg, nil
	}
	// Write a change or exit the watcher.
	put := func(msg *statemachine.Message) {
		select {
		case <-c.Done():
		case cl.commands <- msg:
		}
	}

	getRes, err := cl.kvc.Get(c, prefix, etcdv3.WithPrefix())
	if err != nil {
		metafora.Errorf("Error GETting %s - sending error to stateful handler: %v", prefix, err)
		select {
		case <-c.Done():
			// TODO Do I need the stop channel?
		case <-cl.stop:
		case cl.commands <- statemachine.ErrorMessage(err):
		}
		return nil
	}

	for _, kv := range getRes.Kvs {
		key := string(kv.Key)
		if path.Base(key) == MetadataPath {
			continue
		}
		value := kv.Value
		msg, err := createMessage(key, value)
		if err != nil {
			msg = statemachine.ErrorMessage(err)
		}
		if msg != nil {
			put(msg)
		}
	}

	// Watch deltas in etcd, with the give prefix, starting
	// at the revision of the get call above.
	deltas := cl.etcdv3c.Watch(c, prefix, etcdv3.WithPrefix(), etcdv3.WithRev(getRes.Header.Revision+1), etcdv3.WithFilterDelete())
	for {
		select {
		case <-c.Done():
			return nil
		case <-cl.stop:
			return nil
		case delta, open := <-deltas:
			if !open {
				return ErrWatchClosedUnexpectedly
			}
			if delta.Err() != nil {
				// A non-zero CompactRevision means etcd compacted away the
				// revision we started watching from; report it so watch can
				// re-sync instead of faulting the task.
				if delta.CompactRevision != 0 {
					return ErrWatchCompacted
				}
				return delta.Err()
			}
			for _, event := range delta.Events {
				msg, err := createMessage(string(event.Kv.Key), event.Kv.Value)
				if err != nil {
					msg = statemachine.ErrorMessage(err)
				}
				if msg != nil {
					put(msg)
				}
			}
		}
	}
}
