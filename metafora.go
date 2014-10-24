package metafora

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

var (

	// balance calls are randomized and this is the upper bound of the random
	// amount
	balanceJitterMax = 10 * int64(time.Second)

	//FIXME should probably be improved, see usage in Run()
	consumerRetryDelay = 10 * time.Second
)

type runningTask struct {
	h Handler
	c chan struct{}
}

// Consumer is the core Metafora task runner.
type Consumer struct {
	// Func to create new handlers
	handler HandlerFunc

	// Map of task:Handler
	running map[string]runningTask

	// Mutex to protect access to running
	runL sync.Mutex

	// WaitGroup for running handlers
	hwg sync.WaitGroup

	bal      Balancer
	balEvery time.Duration
	coord    Coordinator
	logger   *logger
	stop     chan struct{} // closed by Shutdown to cause Run to exit

	watch chan string // channel for watcher to send tasks to main loop

	// Set by command handler, read anywhere via Consumer.frozen()
	freezeL sync.Mutex
	freeze  bool
}

// NewConsumer returns a new consumer and calls Init on the Balancer and Coordinator.
func NewConsumer(coord Coordinator, h HandlerFunc, b Balancer) *Consumer {
	c := &Consumer{
		running:  make(map[string]runningTask),
		handler:  h,
		bal:      b,
		balEvery: 15 * time.Minute, //TODO make balance wait configurable
		coord:    coord,
		logger:   stdoutLogger(),
		stop:     make(chan struct{}),
		watch:    make(chan string),
	}

	// initialize balancer with the consumer and a prefixed logger
	b.Init(&struct {
		*Consumer
		Logger
	}{Consumer: c, Logger: c.logger})

	coord.Init(&coordinatorContext{Consumer: c, Logger: c.logger})
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
	c.logger.l = l
	c.logger.lvl = lvl
}

// Run is the core run loop of Metafora. It is responsible for calling into the
// Coordinator to claim work and Balancer to rebalance work.
//
// Run blocks until Shutdown is called.
func (c *Consumer) Run() {
	c.logger.Log(LogLevelDebug, "Starting consumer")

	// chans for core goroutines to communicate with main loop
	balance := make(chan bool)
	cmdChan := make(chan Command)

	// Balance is called by the main loop when the balance channel is ticked
	go func() {
		randInt := rand.New(rand.NewSource(time.Now().UnixNano())).Int63n
		for {
			select {
			case <-c.stop:
				// Shutdown has been called.
				return
			case <-time.After(c.balEvery + time.Duration(randInt(balanceJitterMax))):
				balance <- true
			}
		}
	}()

	// Watch for new tasks in a goroutine
	go c.watcher()

	// Watch for new commands in a goroutine
	go func() {
		for {
			cmd, err := c.coord.Command()
			if err != nil {
				//FIXME add more sophisticated error handling
				c.logger.Log(LogLevelError, "Coordinator returned an error during command, waiting and retrying. %v", err)
				select {
				case <-c.stop:
					return
				case <-time.After(consumerRetryDelay):
				}
				continue
			}
			if cmd == nil {
				c.logger.Log(LogLevelDebug, "Command coordinator exited")
				return
			}
			// Send command to watcher (or shutdown)
			select {
			case <-c.stop:
				return
			case cmdChan <- cmd:
			}
		}
	}()

	// Main Loop ensures events are processed synchronously
	for {
		if c.frozen() {
			// Only recv commands while frozen
			select {
			case <-c.stop:
				// Shutdown has been called.
				return
			case cmd := <-cmdChan:
				c.logger.Log(LogLevelDebug, "Received command: %s", cmd)
				c.handleCommand(cmd)
			}
			continue
		}

		select {
		case <-c.stop:
			// Shutdown has been called.
			return
		case <-balance:
			c.balance()
		case task := <-c.watch:
			if !c.bal.CanClaim(task) {
				c.logger.Log(LogLevelInfo, "Balancer rejected task %s", task)
				break
			}
			if !c.coord.Claim(task) {
				c.logger.Log(LogLevelInfo, "Coordinator unable to claim task %s", task)
				break
			}
			c.claimed(task)
		case cmd := <-cmdChan:
			c.handleCommand(cmd)
		}
	}
}

func (c *Consumer) watcher() {
	c.logger.Log(LogLevelDebug, "Consumer watching")

	for {
		task, err := c.coord.Watch()
		if err != nil {
			//FIXME add more sophisticated error handling
			c.logger.Log(LogLevelError, "Coordinator returned an error during watch, waiting and retrying: %v", err)
			select {
			case <-c.stop:
				return
			case <-time.After(consumerRetryDelay):
			}
			continue
		}
		if task == "" {
			c.logger.Log(LogLevelDebug, "Coordinator has closed, exiting watch loop")
			return
		}
		// Send task to watcher (or shutdown)
		select {
		case <-c.stop:
			return
		case c.watch <- task:
		}
	}
}

func (c *Consumer) balance() {
	for _, task := range c.bal.Balance() {
		//TODO Release tasks asynchronously as their shutdown might be slow?
		c.release(task)
	}
}

// Shutdown stops the main Run loop, calls Stop on all handlers, and calls
// Close on the Coordinator. Running tasks will be released for other nodes to
// claim.
func (c *Consumer) Shutdown() {
	c.logger.Log(LogLevelDebug, "Stopping Run loop")
	close(c.stop)
	c.logger.Log(LogLevelDebug, "Closing Coordinator")
	c.coord.Close()

	// Build list of of currently running tasks
	tasks := c.Tasks()
	c.logger.Log(LogLevelInfo, "Sending stop signal to %d handler(s)", len(tasks))

	// Concurrently shutdown handlers as they may take a while to shutdown
	for _, id := range tasks {
		go c.release(id)
	}

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

// claimed starts a handler for a claimed task. It is the only method to
// manipulate c.running and closes the runningTask channel when a handler's Run
// method exits.
func (c *Consumer) claimed(taskID string) {
	h := c.handler()

	c.logger.Log(LogLevelDebug, "Attempting to start task "+taskID)
	// Associate handler with taskID
	// **This is the only place tasks should be added to c.running**
	c.runL.Lock()
	c.running[taskID] = runningTask{h: h, c: make(chan struct{})}
	c.runL.Unlock()

	c.hwg.Add(1)
	// Start handler in its own goroutine
	go func() {
		defer func() {
			if err := recover(); err != nil {
				c.logger.Log(LogLevelError, "Handler %s panic()'d: %v", taskID, err)
				// panics are considered fatal errors. Make sure the task isn't
				// rescheduled.
				c.coord.Done(taskID)
			}
			// **This is the only place tasks should be removed from c.running**
			c.runL.Lock()
			close(c.running[taskID].c)
			delete(c.running, taskID)
			c.runL.Unlock()
			c.hwg.Done()
		}()

		// Run the task
		c.logger.Log(LogLevelDebug, "Calling run for task %s", taskID)
		if err := h.Run(taskID); err != nil {
			if ferr, ok := err.(FatalError); ok && ferr.Fatal() {
				c.logger.Log(LogLevelError, "Handler for %s exited with fatal error: %v", taskID, err)
			} else {
				c.logger.Log(LogLevelError, "Handler for %s exited with error: %v", taskID, err)
				// error was non-fatal, release and let another node try
				c.coord.Release(taskID)
				return
			}
		}
		c.coord.Done(taskID)
	}()
}

// release stops and Coordinator.Release()s a task if it's running.
//
// release blocks until the task handler stops running.
func (c *Consumer) release(taskID string) {
	// Stop task...
	if c.stopTask(taskID) {
		// ...instruct the coordinator to release it
		c.coord.Release(taskID)
	}
}

// stopTask returns true if the task was running and stopped successfully.
func (c *Consumer) stopTask(taskID string) bool {
	c.runL.Lock()
	task, ok := c.running[taskID]
	c.runL.Unlock()

	if !ok {
		// This can happen if a task completes during Balance() and is not an error.
		c.logger.Log(LogLevelWarn, "Tried to release a non-running task: %s", taskID)
		return false
	}

	// all handler methods must be wrapped in a recover to prevent a misbehaving
	// handler from crashing the entire consumer
	func() {
		defer func() {
			if err := recover(); err != nil {
				c.logger.Log(LogLevelError, "Handler %s panic()'d on Stop: %v", taskID, err)
			}
		}()

		task.h.Stop()
	}()

	// Once the handler is stopped...
	//FIXME should there be a timeout here?
	<-task.c
	return true
}

func (c *Consumer) frozen() bool {
	c.freezeL.Lock()
	r := c.freeze
	c.freezeL.Unlock()
	return r
}

func (c *Consumer) handleCommand(cmd Command) {
	switch cmd.Name() {
	case cmdFreeze:
		if c.frozen() {
			c.logger.Log(LogLevelInfo, "Ignoring freeze command: already frozen")
			return
		}
		c.logger.Log(LogLevelInfo, "Freezing")
		c.freezeL.Lock()
		c.freeze = true
		c.freezeL.Unlock()
	case cmdUnfreeze:
		if !c.frozen() {
			c.logger.Log(LogLevelInfo, "Ignoring unfreeze command: not frozen")
			return
		}
		c.logger.Log(LogLevelInfo, "Unfreezing")
		c.freezeL.Lock()
		c.freeze = false
		c.freezeL.Unlock()
	case cmdBalance:
		c.logger.Log(LogLevelInfo, "Balancing due to command")
		c.balance()
		c.logger.Log(LogLevelDebug, "Finished balancing due to command")
	case cmdReleaseTask:
		taskI, ok := cmd.Parameters()["task"]
		task, ok2 := taskI.(string)
		if !ok || !ok2 {
			c.logger.Log(LogLevelError, "Release task command didn't contain a valid task")
			return
		}
		c.logger.Log(LogLevelInfo, "Releasing task %s due to command", task)
		c.release(task)
	case cmdStopTask:
		taskI, ok := cmd.Parameters()["task"]
		task, ok2 := taskI.(string)
		if !ok || !ok2 {
			c.logger.Log(LogLevelError, "Stop task command didn't contain a valid task")
			return
		}
		c.logger.Log(LogLevelInfo, "Stopping task %s due to command", task)
		c.stopTask(task)
	default:
		c.logger.Log(LogLevelWarn, "Discarding unknown command: %s", cmd.Name())
	}
}
