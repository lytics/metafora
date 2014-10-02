package metafora

import (
	"log"
	"os"
	"sort"
	"sync"
)

// Consumer is the core Metafora task runner.
type Consumer struct {
	// Func to create new handlers
	handler HandlerFunc

	// Map of task:Handler
	running map[string]Handler

	// Mutex to protect access to running
	runL sync.Mutex

	// WaitGroup for running handlers
	hwg sync.WaitGroup

	bal    Balancer
	coord  Coordinator
	logger Logger
}

// NewConsumer returns a new consumer and calls Init on the Balancer and Coordinator.
func NewConsumer(coord Coordinator, h HandlerFunc, b Balancer) *Consumer {
	c := &Consumer{
		running: make(map[string]Handler),
		handler: h,
		bal:     b,
		coord:   coord,
		logger:  &logger{l: log.New(os.Stderr, "", log.Flags()), lvl: LogLevelInfo},
	}

	// initialize balancer with the consumer and a prefixed logger
	b.Init(&struct {
		*Consumer
		Logger
	}{Consumer: c, Logger: newPrefixLogger(c.logger, "balancer:")})

	// initialize coordinator with a logger
	coord.Init(newPrefixLogger(c.logger, "coordinator:"))
	return c
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (c *Consumer) SetLogger(l logOutputter, lvl LogLevel) {
	c.logger = &logger{l: l, lvl: lvl}
}

func (c *Consumer) Start() {
	//TODO start etcd watches and call claimed for each one
}

func (c *Consumer) Shutdown() {
	c.logger.Log(LogLevelInfo, "Sending stop signal to handlers")
	// Concurrently shutdown handles
	wg := sync.WaitGroup{}
	wg.Add(len(c.running))
	for id, h := range c.running {
		go func(gid string, gh Handler) {
			gh.Stop()

			// Release tasks that cleanly stopped
			c.release(gid)
			wg.Done()
		}(id, h)
	}
	//TODO timeout?
	wg.Wait()

	c.logger.Log(LogLevelInfo, "Waiting for handlers to exit")
	c.hwg.Wait()
}

// Tasks returns a sorted list of running Task IDs.
func (c *Consumer) Tasks() []string {
	c.runL.Lock()
	defer c.runL.Unlock()
	t := make([]string, len(c.running))
	i := 0
	for id, _ := range c.running {
		t[i] = id
		i++
	}
	sort.Strings(t)
	return t
}

//TODO This needs to be split into the coord.Watch/bal.CanClaim call and the
//     coord.Claim/h.Run call.
func (c *Consumer) claimed(taskID string) {

	if !c.bal.CanClaim(taskID) {
		return
	}

	// Create handler
	h := c.handler()

	// Associate handler with taskID
	c.runL.Lock()
	c.running[taskID] = h
	c.runL.Unlock()

	c.hwg.Add(1)
	// Start handler in its own goroutine
	go func() {
		defer func() {
			if err := recover(); err != nil {
				c.logger.Log(LogLevelError, "Handler %s panic()'d: %v", taskID, err)
			}
			c.runL.Lock()
			delete(c.running, taskID)
			c.runL.Unlock()
			c.hwg.Done()
		}()

		if err := h.Run(taskID); err != nil {
			c.logger.Log(LogLevelError, "Handler for %s exited with error: %v", taskID, err)
		}
	}()
}

func (c *Consumer) release(taskID string) {
	//TODO release task ID
}
