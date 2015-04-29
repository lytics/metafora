package embedded

import (
	"fmt"

	"github.com/lytics/metafora/statemachine"
)

var (
	_ statemachine.Commander       = (*Commander)(nil)
	_ statemachine.CommandListener = (*commandListener)(nil)
)

type Commander struct {
	listeners map[string]chan statemachine.Message
}

func NewCommander() *Commander {
	return &Commander{listeners: make(map[string]chan statemachine.Message)}
}

func (c *Commander) NewListener(taskID string) statemachine.CommandListener {
	// Buffer chan to make sending/recving asynchronous
	c.listeners[taskID] = make(chan statemachine.Message, 1)
	return &commandListener{c: c.listeners[taskID]}
}

func (c *Commander) Send(taskID string, m statemachine.Message) error {
	cl, ok := c.listeners[taskID]
	if !ok {
		return fmt.Errorf("task=%q not running", taskID)
	}
	cl <- m
	return nil
}

type commandListener struct {
	c <-chan statemachine.Message
}

func (cl *commandListener) Receive() <-chan statemachine.Message { return cl.c }
func (*commandListener) Stop()                                   {}
