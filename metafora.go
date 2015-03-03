package metafora

import (
	"math/rand"
	"runtime"
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

// Consumer is the core Metafora task runner.
type Consumer struct {
	// Func to create new handlers
	handler HandlerFunc

	// Map of task:Handler
	running map[string]*task

	// Mutex to protect access to running
	runL sync.Mutex

	// WaitGroup for running handlers and consumer goroutines
	hwg sync.WaitGroup

	// WaitGroup so Shutdown() can block on Run() exiting fully
	runwg  sync.WaitGroup
	runwgL sync.Mutex

	bal      Balancer
	balEvery time.Duration
	coord    Coordinator
	stop     chan struct{} // closed by Shutdown to cause Run to exit

	// ticked on each loop of the main loop to enforce sequential interaction
	// with coordinator and balancer
	tick chan int

	watch chan string // channel for watcher to send tasks to main loop

	// Set by command handler, read anywhere via Consumer.frozen()
	freezeL sync.Mutex
	freeze  bool
}

// NewConsumer returns a new consumer and calls Init on the Balancer and Coordinator.
func NewConsumer(coord Coordinator, h HandlerFunc, b Balancer) (*Consumer, error) {
	c := &Consumer{
		running:  make(map[string]*task),
		handler:  h,
		bal:      b,
		balEvery: 15 * time.Minute, //TODO make balance wait configurable
		coord:    coord,
		stop:     make(chan struct{}),
		tick:     make(chan int),
		watch:    make(chan string),
	}

	// initialize balancer with the consumer and a prefixed logger
	b.Init(c)

	if err := coord.Init(&coordinatorContext{c}); err != nil {
		return nil, err
	}
	return c, nil
}

// Run is the core run loop of Metafora. It is responsible for calling into the
// Coordinator to claim work and Balancer to rebalance work.
//
// Run blocks until Shutdown is called or an internal error occurs.
func (c *Consumer) Run() {
	Debug("Starting consumer")

	// Increment run wait group so Shutdown() can block on Run() exiting fully.
	c.runwgL.Lock()
	c.runwg.Add(1)
	c.runwgL.Unlock()
	defer c.runwg.Done()

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
				Info("Balancing")
				select {
				case balance <- true:
					// Ticked balance
				case <-c.stop:
					// Shutdown has been called.
					return
				}
			}
			// Wait for main loop to signal balancing is done
			select {
			case <-c.stop:
				return
			case <-c.tick:
			}
		}
	}()

	// Watch for new tasks in a goroutine
	go c.watcher()

	// Watch for new commands in a goroutine
	go func() {
		defer close(cmdChan)
		for {
			cmd, err := c.coord.Command()
			if err != nil {
				Errorf("Coordinator returned an error during command, waiting and retrying. %v", err)
				select {
				case <-c.stop:
					return
				case <-time.After(consumerRetryDelay):
				}
				continue
			}
			if cmd == nil {
				Debug("Command coordinator exited")
				return
			}
			// Send command to watcher (or shutdown)
			select {
			case <-c.stop:
				return
			case cmdChan <- cmd:
			}
			// Wait for main loop to signal command has been handled
			select {
			case <-c.stop:
				return
			case <-c.tick:
			}
		}
	}()

	// Make sure Run() cleans up on exit (stops coordinator, releases tasks, etc)
	defer c.shutdown()

	// Main Loop ensures events are processed synchronously
	for {
		if c.Frozen() {
			// Only recv commands while frozen
			select {
			case <-c.stop:
				// Shutdown has been called.
				return
			case cmd, ok := <-cmdChan:
				if !ok {
					Debug("Command channel closed. Exiting main loop.")
					return
				}
				Debugf("Received command: %s", cmd)
				c.handleCommand(cmd)
			}
			// Must send tick whenever main loop restarts
			select {
			case <-c.stop:
				return
			case c.tick <- 1:
			}
			continue
		}

		select {
		case <-c.stop:
			// Shutdown has been called.
			return
		case <-balance:
			c.balance()
		case task, ok := <-c.watch:
			if !ok {
				Debug("Watch channel closed. Exiting main loop.")
				return
			}
			if !c.bal.CanClaim(task) {
				Infof("Balancer rejected task %s", task)
				break
			}
			if !c.coord.Claim(task) {
				Infof("Coordinator unable to claim task %s", task)
				break
			}
			c.claimed(task)
		case cmd, ok := <-cmdChan:
			if !ok {
				Debug("Command channel closed. Exiting main loop.")
				return
			}
			c.handleCommand(cmd)
		}
		// Signal that main loop is restarting after handling an event
		select {
		case <-c.stop:
			return
		case c.tick <- 1:
		}
	}
}

func (c *Consumer) watcher() {
	defer close(c.watch)
	Debug("Consumer watching")

	for {
		task, err := c.coord.Watch()
		if err != nil {
			//FIXME add more sophisticated error handling
			Errorf("Coordinator returned an error during watch, waiting and retrying: %v", err)
			select {
			case <-c.stop:
				return
			case <-time.After(consumerRetryDelay):
			}
			continue
		}
		if task == "" {
			Info("Coordinator has closed, no longer watching for tasks.")
			return
		}
		// Send task to watcher (or shutdown)
		select {
		case <-c.stop:
			return
		case c.watch <- task:
		}
		// Wait for main loop to signal task has been handled
		select {
		case <-c.stop:
			return
		case <-c.tick:
		}
	}
}

func (c *Consumer) balance() {
	tasks := c.bal.Balance()
	if len(tasks) > 0 {
		Infof("Balancer releasing: %v", tasks)
	}
	for _, task := range tasks {
		c.stopTask(task)
	}
}

// shutdown is the actual shutdown logic called when Run() exits.
func (c *Consumer) shutdown() {
	// Build list of of currently running tasks
	tasks := c.Tasks()
	Infof("Sending stop signal to %d handler(s)", len(tasks))

	for _, id := range tasks {
		c.stopTask(id.ID())
	}

	Info("Waiting for handlers to exit")
	c.hwg.Wait()

	Debug("Closing Coordinator")
	c.coord.Close()
}

// Shutdown stops the main Run loop, calls Stop on all handlers, and calls
// Close on the Coordinator. Running tasks will be released for other nodes to
// claim.
func (c *Consumer) Shutdown() {
	// acquire the runL lock to make sure we don't race with claimed()'s <-c.stop
	// check
	c.runL.Lock()
	select {
	case <-c.stop:
		// already stopped
	default:
		Debug("Stopping Run loop")
		close(c.stop)
	}
	c.runL.Unlock()

	// Wait for task handlers to exit.
	c.hwg.Wait()

	// Make sure Run() exits, otherwise Shutdown() might exit before
	// coord.Close() is called.
	c.runwgL.Lock()
	c.runwg.Wait()
	c.runwgL.Unlock()
}

// Tasks returns a lexicographically sorted list of running Task IDs.
func (c *Consumer) Tasks() []Task {
	c.runL.Lock()
	defer c.runL.Unlock()

	// Create a sorted list of task IDs
	ids := make([]string, len(c.running))
	i := 0
	for id, _ := range c.running {
		ids[i] = id
		i++
	}
	sort.Strings(ids)

	// Add tasks in lexicographic order
	t := make([]Task, len(ids))
	for i, id := range ids {
		t[i] = c.running[id]
	}
	return t
}

// claimed starts a handler for a claimed task. It is the only method to
// manipulate c.running and closes the task channel when a handler's Run
// method exits.
func (c *Consumer) claimed(taskID string) {
	h := c.handler()

	Debugf("Attempting to start task " + taskID)
	// Associate handler with taskID
	// **This is the only place tasks should be added to c.running**
	c.runL.Lock()
	defer c.runL.Unlock()
	select {
	case <-c.stop:
		// We're closing, don't bother starting this task
		return
	default:
	}
	if _, ok := c.running[taskID]; ok {
		// If a coordinator returns an already claimed task from Watch(), then it's
		// a coordinator (or broker) bug.
		Warnf("Attempted to claim already running task %s", taskID)
		return
	}
	rt := newTask(taskID, h)
	c.running[taskID] = rt

	// This must be done in the runL lock after the stop chan check so Shutdown
	// doesn't close(stop) and start Wait()ing concurrently.
	// See "Note" http://golang.org/pkg/sync/#WaitGroup.Add
	c.hwg.Add(1)

	// Start handler in its own goroutine
	go func() {
		defer c.hwg.Done() // Must be run after task exit and Done/Release called

		// Run the task
		Infof("Task started: %s", taskID)
		done := c.runTask(h.Run, taskID)
		if done {
			Infof("Task exited: %s (marking done)", taskID)
			c.coord.Done(taskID)
		} else {
			Infof("Task exited: %s (releasing)", taskID)
			c.coord.Release(taskID)
		}
	}()
}

// runTask executes a handler's Run method and recovers from panic()s.
func (c *Consumer) runTask(run func(string) bool, task string) bool {
	done := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				stack := make([]byte, 50*1024)
				sz := runtime.Stack(stack, false)
				Errorf("Handler %s panic()'d: %v\n%s", task, err, stack[:sz])
				// panics are considered fatal errors. Make sure the task isn't
				// rescheduled.
				done = true
			}

			// **This is the only place tasks should be removed from c.running**
			c.runL.Lock()
			delete(c.running, task)
			c.runL.Unlock()
		}()
		done = run(task)
	}()
	return done
}

// stopTask asynchronously calls the task handlers' Stop method. While stopTask
// calls don't block, calls to task handler's Stop method are serialized with a
// lock.
func (c *Consumer) stopTask(taskID string) {
	c.runL.Lock()
	task, ok := c.running[taskID]
	c.runL.Unlock()

	if !ok {
		// This can happen if a task completes during Balance() and is not an error.
		Warnf("Tried to release a non-running task: %s", taskID)
		return
	}

	// all handler methods must be wrapped in a recover to prevent a misbehaving
	// handler from crashing the entire consumer
	go func() {
		defer func() {
			if err := recover(); err != nil {
				stack := make([]byte, 50*1024)
				sz := runtime.Stack(stack, false)
				Errorf("Handler %s panic()'d on Stop: %v\n%s", taskID, err, stack[:sz])
			}
		}()

		// Serialize calls to Stop as a convenience to handler implementors.
		task.stop()
	}()
}

// Frozen returns true if Metafora is no longer watching for new tasks or
// rebalancing.
//
// Metafora will remain frozen until receiving an Unfreeze command or it is
// restarted (frozen state is not persisted).
func (c *Consumer) Frozen() bool {
	c.freezeL.Lock()
	r := c.freeze
	c.freezeL.Unlock()
	return r
}

func (c *Consumer) handleCommand(cmd Command) {
	switch cmd.Name() {
	case cmdFreeze:
		if c.Frozen() {
			Info("Ignoring freeze command: already frozen")
			return
		}
		Info("Freezing")
		c.freezeL.Lock()
		c.freeze = true
		c.freezeL.Unlock()
	case cmdUnfreeze:
		if !c.Frozen() {
			Info("Ignoring unfreeze command: not frozen")
			return
		}
		Info("Unfreezing")
		c.freezeL.Lock()
		c.freeze = false
		c.freezeL.Unlock()
	case cmdBalance:
		Info("Balancing due to command")
		c.balance()
		Debug("Finished balancing due to command")
	case cmdStopTask:
		taskI, ok := cmd.Parameters()["task"]
		task, ok2 := taskI.(string)
		if !ok || !ok2 {
			Error("Stop task command didn't contain a valid task")
			return
		}
		Info("Stopping task %s due to command", task)
		c.stopTask(task)
	default:
		Warnf("Discarding unknown command: %s", cmd.Name())
	}
}
