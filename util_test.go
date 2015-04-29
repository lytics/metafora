package metafora

import "errors"

//TODO Move out into a testutil package for other packages to use. The problem
//is that existing metafora tests would have to be moved to the metafora_test
//package which means no manipulating unexported globals like balance jitter.

type TestCoord struct {
	Name     string
	Tasks    chan string // will be returned in order, "" indicates return an error
	Commands chan Command
	Releases chan string
	Dones    chan string
	closed   chan bool
}

func NewTestCoord() *TestCoord {
	return &TestCoord{
		Name:     "testcoord",
		Tasks:    make(chan string, 10),
		Commands: make(chan Command, 10),
		Releases: make(chan string, 10),
		Dones:    make(chan string, 10),
		closed:   make(chan bool),
	}
}

func (*TestCoord) Init(CoordinatorContext) error { return nil }
func (*TestCoord) Claim(string) bool             { return true }
func (c *TestCoord) Close()                      { close(c.closed) }
func (c *TestCoord) Release(task string)         { c.Releases <- task }
func (c *TestCoord) Done(task string)            { c.Dones <- task }
func (c *TestCoord) String() string              { return c.Name }

// Watch sends tasks from the Tasks channel unless an empty string is sent.
// Then an error is returned.
func (c *TestCoord) Watch(out chan<- string) error {
	task := ""
	for {
		select {
		case task = <-c.Tasks:
			Debugf("TestCoord recvd: %s", task)
			if task == "" {
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
	return nil
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
