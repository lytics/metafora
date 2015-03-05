package metafora

import "errors"

//TODO Move out into a testutil package for other packages to use. The problem
//is that existing metafora tests would have to be moved to the metafora_test
//package which means no manipulating unexported globals like balance jitter.

var bal = &DumbBalancer{}

type TestCoord struct {
	Tasks    chan string // will be returned in order, "" indicates return an error
	Commands chan Command
	Releases chan string
	Dones    chan string
}

func NewTestCoord() *TestCoord {
	return &TestCoord{
		Tasks:    make(chan string, 10),
		Commands: make(chan Command, 10),
		Releases: make(chan string, 10),
		Dones:    make(chan string, 10),
	}
}

func (*TestCoord) Init(CoordinatorContext) error { return nil }
func (*TestCoord) Claim(string) bool             { return true }
func (*TestCoord) Close()                        { return }
func (c *TestCoord) Release(task string)         { c.Releases <- task }
func (c *TestCoord) Done(task string)            { c.Dones <- task }

// Watch returns tasks from the Tasks channel unless an empty string is sent.
// Then an error is returned.
func (c *TestCoord) Watch() (string, error) {
	task := <-c.Tasks
	if task == "" {
		return "", errors.New("test error")
	}
	return task, nil
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
