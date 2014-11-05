package embedded

import (
	"errors"

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
				break
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
}

func (e *EmbeddedCoordinator) Init(c metafora.CoordinatorContext) error {
	e.ctx = c
	return nil
}

func (e *EmbeddedCoordinator) Watch() (taskID string, err error) {

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
	e.inchan <- taskID
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
