package embedded

import (
	"errors"
	"sync"

	"github.com/lytics/metafora"
)

func NewEmbeddedCoordinator(nodeid string, taskchan chan string, cmdchan chan *NodeCommand, nodechan chan []string) metafora.Coordinator {
	e := &EmbeddedCoordinator{inchan: taskchan, cmdchan: cmdchan, stopchan: make(chan struct{}), nodechan: nodechan}
	// HACK - need to respond to node requests, assuming a single coordinator/client pair
	go func() {
		for {
			select {
			case e.nodechan <- []string{e.nodeid}:
			case <-e.stopchan:
				return
			}
		}
	}()

	return e
}

// Coordinator which listens for tasks on a channel
type EmbeddedCoordinator struct {
	nodeid   string
	ctx      metafora.CoordinatorContext
	inchan   chan string
	cmdchan  chan *NodeCommand
	nodechan chan<- []string
	stopchan chan struct{}

	bl      sync.Mutex
	backlog []string
}

func (e *EmbeddedCoordinator) Init(c metafora.CoordinatorContext) error {
	e.ctx = c
	return nil
}

func (e *EmbeddedCoordinator) Watch() (taskID string, err error) {
	// first check backlog for tasks
	e.bl.Lock()
	if len(e.backlog) > 0 {
		taskID, e.backlog = e.backlog[len(e.backlog)-1], e.backlog[:len(e.backlog)-1]
		e.bl.Unlock()
		return taskID, nil
	}
	e.bl.Unlock()

	// wait for incoming tasks
	select {
	case id, ok := <-e.inchan:
		if !ok {
			return "", errors.New("Input closed")
		}
		return id, nil
	case <-e.stopchan:
		return "", nil
	}
}

func (e *EmbeddedCoordinator) Claim(taskID string) bool {
	// We recieved on a channel, we are the only ones to pull that value
	return true
}

func (e *EmbeddedCoordinator) Release(taskID string) {
	select {
	case e.inchan <- taskID:
	case <-e.stopchan:
	default:
		// No consumers watching and not stopping, store in backlog
		e.bl.Lock()
		e.backlog = append(e.backlog, taskID)
		e.bl.Unlock()
	}
}

func (e *EmbeddedCoordinator) Done(taskID string) {}

func (e *EmbeddedCoordinator) Command() (metafora.Command, error) {
	select {
	case cmd, ok := <-e.cmdchan:
		if !ok {
			return nil, errors.New("Cmd channel closed")
		}
		return cmd.Cmd, nil
	case <-e.stopchan:
		return nil, nil
	}
}

func (e *EmbeddedCoordinator) Close() {
	close(e.stopchan)
}
