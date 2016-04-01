package m_etcd

import (
	"encoding/json"
	"path"
	"sync"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
	"golang.org/x/net/context"
)

const (
	commandPath = "commands"

	// cmdTTL is the TTL in seconds set on commands so that commands sent to
	// terminating work aren't orphaned in etcd forever.
	cmdTTL = 7 * 24 * time.Hour
)

type cmdr struct {
	cli  client.KeysAPI
	path string
}

func NewCommander(namespace string, etcdclient client.KeysAPI) statemachine.Commander {
	if namespace[0] != '/' {
		namespace = "/" + namespace
	}
	return &cmdr{path: path.Join(namespace, commandPath), cli: etcdclient}
}

// Send command to a task. Overwrites existing commands.
func (c *cmdr) Send(taskID string, m *statemachine.Message) error {
	buf, err := json.Marshal(m)
	if err != nil {
		return err
	}

	opts := &client.SetOptions{TTL: cmdTTL}
	_, err = c.cli.Set(context.TODO(), path.Join(c.path, taskID), string(buf), opts)
	return err
}

type cmdrListener struct {
	cli  client.KeysAPI
	path string

	commands chan *statemachine.Message

	mu   *sync.Mutex
	stop chan bool
}

// NewCommandListener makes a statemachine.CommandListener implementation
// backed by etcd. The namespace should be the same as the coordinator as
// commands use a separate path within a namespace than tasks or nodes.
func NewCommandListener(task metafora.Task, namespace string, kc client.KeysAPI) statemachine.CommandListener {
	if namespace[0] != '/' {
		namespace = "/" + namespace
	}
	cl := &cmdrListener{
		path:     path.Join(namespace, commandPath, task.ID()),
		cli:      kc,
		commands: make(chan *statemachine.Message),
		mu:       &sync.Mutex{},
		stop:     make(chan bool),
	}
	go cl.watcher()
	return cl
}

func (c *cmdrListener) Receive() <-chan *statemachine.Message { return c.commands }
func (c *cmdrListener) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
}

func (c *cmdrListener) sendErr(err error) {
	select {
	case c.commands <- statemachine.ErrorMessage(err):
	case <-c.stop:
	}
}

func (c *cmdrListener) sendMsg(resp *client.Response) (ok bool) {
	// Delete/Expire events shouldn't be processed
	if releaseActions[resp.Action] {
		return true
	}

	// Remove command so it's not processed twice
	opts := &client.DeleteOptions{PrevValue: resp.Node.Value}
	if _, err := c.cli.Delete(context.TODO(), resp.Node.Key, opts); err != nil {
		if ee, ok := err.(*client.Error); ok && ee.Code == client.ErrorCodeTestFailed {
			metafora.Infof("Received successive commands; attempting to retrieve the latest: %v", err)
			return true
		}
		metafora.Errorf("Error deleting command %s: %s - sending error to stateful handler: %v", c.path, resp.Node.Value, err)
		c.sendErr(err)
		return false
	}

	msg := &statemachine.Message{}
	if err := json.Unmarshal([]byte(resp.Node.Value), msg); err != nil {
		metafora.Errorf("Error unmarshalling command from %s - sending error to stateful handler: %v", c.path, err)
		c.sendErr(err)
		return false
	}

	select {
	case c.commands <- msg:
		return true
	case <-c.stop:
		return false
	}
}

func (c *cmdrListener) watcher() {
	opts := &client.WatcherOptions{Recursive: true}

startWatch:
	resp, err := c.cli.Get(context.TODO(), c.path, getNoSortNoRecur)
	if err != nil {
		if ee, ok := err.(client.Error); ok && ee.Code == client.ErrorCodeKeyNotFound {
			// No command found; this is normal. Grab index and skip to watching
			opts.AfterIndex = ee.Index
			goto watchLoop
		}
		metafora.Errorf("Error GETting %s - sending error to stateful handler: %v", c.path, err)
		c.sendErr(err)
		return
	}

	// Existing command found, send it and start watching after it
	if !c.sendMsg(resp) {
		return
	}
	opts.AfterIndex = resp.Index

watchLoop:
	watcher := c.cli.Watcher(c.path, opts)
	for {
		//TODO remove once Watcher's context checks ec.stop
		select {
		case <-c.stop:
			return
		default:
		}

		//TODO use ec.stop in context
		resp, err := watcher.Next(context.TODO())
		if err != nil {
			if ee, ok := err.(*client.Error); ok && ee.Code == client.ErrorCodeEventIndexCleared {
				// Need to restart watch with a new Get
				goto startWatch
			}

			//FIXME what error is this replaced by?!
			if err.Error() == "ErrWatchStoppedByUser" {
				return
			}
			metafora.Errorf("Error watching %s - sending error to stateful handler: %v", c.path, err)
			c.sendErr(err)
			return
		}

		metafora.Debugf("Received command via %s -- sending to statemachine", c.path)
		if !c.sendMsg(resp) {
			return
		}
	}
}
