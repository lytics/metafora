package metafora

import "errors"

//TODO Move out into a testutil package for other packages to use. The problem
//is that existing metafora tests would have to be moved to the metafora_test
//package which means no manipulating unexported globals like balance jitter.

type TestCoord struct {
	name     string
	Tasks    chan Task // will be returned in order, "" indicates return an error
	Commands chan Command
	Releases chan Task
	Dones    chan Task
	closed   chan bool
}

func NewTestCoord() *TestCoord {
	return &TestCoord{
		name:     "testcoord",
		Tasks:    make(chan Task, 10),
		Commands: make(chan Command, 10),
		Releases: make(chan Task, 10),
		Dones:    make(chan Task, 10),
		closed:   make(chan bool),
	}
}

func (*TestCoord) Init(CoordinatorContext) error { return nil }
func (*TestCoord) Claim(Task) bool               { return true }
func (c *TestCoord) Close()                      { close(c.closed) }
func (c *TestCoord) Release(task Task)           { c.Releases <- task }
func (c *TestCoord) Done(task Task)              { c.Dones <- task }
func (c *TestCoord) Name() string                { return c.name }

// Watch sends tasks from the Tasks channel unless an empty string is sent.
// Then an error is returned.
func (c *TestCoord) Watch(out chan<- Task) error {
	var task Task
	for {
		select {
		case task = <-c.Tasks:
			Debugf("TestCoord recvd: %s", task)
			if task == nil || task.ID() == "" {
				return errors.New("test error")
			}
		case <-c.closed:
			return nil
		}
		select {
		case out <- task:
			Debugf("TestCoord sent: %s", task)
		case <-c.closed:
			return nil
		}
	}
}

// Command returns commands from the Commands channel unless a nil is sent.
// Then an error is returned.
func (c *TestCoord) Command() (Command, error) {
	cmd := <-c.Commands
	if cmd == nil {
		return cmd, errors.New("test error")
	}
	return cmd, nil
}
