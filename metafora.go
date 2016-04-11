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
)

// Consumer is the core Metafora task runner.
type Consumer struct {
	// Func to create new handlers
	handler HandlerFunc

	// Map of task:Handler
	running map[string]*runtask

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
	im       *ignoremgr
	stop     chan struct{} // closed by Shutdown to cause Run to exit
	tasks    chan Task     // channel for watcher to send tasks to main loop

	// Set by command handler, read anywhere via Consumer.frozen()
	freezeL sync.Mutex
	freeze  bool
}

// NewConsumer returns a new consumer and calls Init on the Balancer and Coordinator.
func NewConsumer(coord Coordinator, h HandlerFunc, b Balancer) (*Consumer, error) {
	c := &Consumer{
		running:  make(map[string]*runtask),
		handler:  h,
		bal:      b,
		balEvery: 15 * time.Minute, //TODO make balance wait configurable
		coord:    coord,
		stop:     make(chan struct{}),
		tasks:    make(chan Task),
	}
	c.im = ignorer(c.tasks, c.stop)

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
	Debug(c, " Starting consumer")

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
				Info(c, " Balancing")
				select {
				case balance <- true:
					// Ticked balance
				case <-c.stop:
					// Shutdown has been called.
					return
				}
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
				Errorf("Exiting because coordinator returned an error during command: %v", err)
				return
			}
			if cmd == nil {
				Debug(c, " Command coordinator exited")
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
					Debug(c, " Command channel closed. Exiting main loop.")
					return
				}
				Debugf("%s Received command: %s", c, cmd)
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
		case task := <-c.tasks:
			tid := task.ID()
			if c.ignored(tid) {
				Debugf("%s task=%q ignored", c, tid)
				continue
			}
			if until, ok := c.bal.CanClaim(task); !ok {
				Infof("%s Balancer rejected task=%q until %s", c, tid, until)
				c.ignore(task, until)
				break
			}
			if !c.coord.Claim(task) {
				Debugf("%s Coordinator unable to claim task=%q", c, tid)
				break
			}
			c.claimed(task)
			//FIXME Temporary hack to fix claim backoffs
			time.Sleep(10 * time.Millisecond)
		case cmd, ok := <-cmdChan:
			if !ok {
				Debug(c, " Command channel closed. Exiting main loop.")
				return
			}
			c.handleCommand(cmd)
		}
	}
}

func (c *Consumer) watcher() {
	// The watcher dying unexpectedly should close the consumer to cause a
	// shutdown.
	defer c.close()

	err := c.coord.Watch(c.tasks)
	if err != nil {
		Errorf("Exiting because coordinator returned an error during watch: %v", err)
		return
	}
}

func (c *Consumer) balance() {
	tasks := c.bal.Balance()
	if len(tasks) > 0 {
		Infof("%s Balancer releasing: %v", c, tasks)
	}
	for _, task := range tasks {
		// Actually release the rebalanced task.
		c.stopTask(task)
	}
}

// close the c.stop channel which signals for the consumer to shutdown.
func (c *Consumer) close() {
	// acquire the runL lock to make sure we don't race with claimed()'s <-c.stop
	// check
	c.runL.Lock()
	defer c.runL.Unlock()
	select {
	case <-c.stop:
		// already stopped
	default:
		Debug("Stopping Run loop")
		close(c.stop)
	}
}

// shutdown is the actual shutdown logic called when Run() exits.
func (c *Consumer) shutdown() {
	Debug("Closing Coordinator ", c)
	c.coord.Close()

	// Build list of of currently running tasks
	runningtasks := c.Tasks()
	Infof("Coordinator closed. Sending stop signal to %d handler(s)", len(runningtasks))

	for _, rt := range runningtasks {
		c.stopTask(rt.Task().ID())
	}

	Info(c, " Waiting for handlers to exit")
	c.hwg.Wait()
}

// Shutdown stops the main Run loop, calls Stop on all handlers, and calls
// Close on the Coordinator. Running tasks will be released for other nodes to
// claim.
func (c *Consumer) Shutdown() {
	c.close()

	// Wait for task handlers to exit.
	c.hwg.Wait()

	// Make sure Run() exits, otherwise Shutdown() might exit before
	// coord.Close() is called.
	c.runwgL.Lock()
	c.runwg.Wait()
	c.runwgL.Unlock()
}

// Tasks returns a lexicographically sorted list of running Task IDs.
func (c *Consumer) Tasks() []RunningTask {
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
	t := make([]RunningTask, len(ids))
	for i, id := range ids {
		t[i] = c.running[id]
	}
	return t
}

// claimed starts a handler for a claimed task. It is the only method to
// manipulate c.running and closes the task channel when a handler's Run
// method exits.
func (c *Consumer) claimed(task Task) {
	h := c.handler(task)

	tid := task.ID()
	Debugf("%s is attempting to start task=%q", c, tid)
	// Associate handler with taskID
	// **This is the only place tasks should be added to c.running**
	c.runL.Lock()
	defer c.runL.Unlock()
	select {
	case <-c.stop:
		// We're closing, don't bother starting this task
		c.coord.Release(task)
		return
	default:
	}
	if _, ok := c.running[tid]; ok {
		// If a coordinator returns an already claimed task from Watch(), then it's
		// a coordinator (or broker) bug.
		Warnf("%s Attempted to claim already running task %s", c, tid)
		return
	}
	rt := newTask(task, h)
	c.running[tid] = rt

	// This must be done in the runL lock after the stop chan check so Shutdown
	// doesn't close(stop) and start Wait()ing concurrently.
	// See "Note" http://golang.org/pkg/sync/#WaitGroup.Add
	c.hwg.Add(1)

	// Start handler in its own goroutine
	go func() {
		defer c.hwg.Done() // Must be run after task exit and Done/Release called

		// Run the task
		Infof("%s Task %q started", c, tid)
		done := c.runTask(h.Run, tid)
		var status string
		if done {
			status = "done"
			c.coord.Done(task)
		} else {
			status = "released"
			c.coord.Release(task)
		}

		stopped := rt.Stopped()
		if stopped.IsZero() {
			// Task exited on its own
			Infof("%s Task %q exited (%s)", c, tid, status)
		} else {
			// Task exited due to Stop() being called
			Infof("%s Task %q exited (%s) after %s", c, tid, status, time.Now().Sub(stopped))
		}

		// **This is the only place tasks should be removed from c.running**
		c.runL.Lock()
		delete(c.running, tid)
		c.runL.Unlock()
	}()

	// Pause slightly after a successful claim to give starting tasks some
	// breathing room and to bias the next claim toward a node that lost this
	// one.
	time.Sleep(10 * time.Millisecond)
}

// runTask executes a handler's Run method and recovers from panic()s.
func (c *Consumer) runTask(run func() bool, task string) bool {
	done := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				stack := make([]byte, 50*1024)
				sz := runtime.Stack(stack, false)
				Errorf("%s Handler %s panic()'d: %v\n%s", c, task, err, stack[:sz])
				// panics are considered fatal errors. Make sure the task isn't
				// rescheduled.
				done = true
			}
		}()
		done = run()
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
		Warnf("%s tried to release a non-running task=%q", c, taskID)
		return
	}

	// all handler methods must be wrapped in a recover to prevent a misbehaving
	// handler from crashing the entire consumer
	go func() {
		defer func() {
			if err := recover(); err != nil {
				stack := make([]byte, 50*1024)
				sz := runtime.Stack(stack, false)
				Errorf("%s Handler %s panic()'d on Stop: %v\n%s", c, taskID, err, stack[:sz])
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
			Info(c, " Ignoring freeze command: already frozen")
			return
		}
		Info(c, " Freezing")
		c.freezeL.Lock()
		c.freeze = true
		c.freezeL.Unlock()
	case cmdUnfreeze:
		if !c.Frozen() {
			Info(c, " Ignoring unfreeze command: not frozen")
			return
		}
		Info(c, " Unfreezing")
		c.freezeL.Lock()
		c.freeze = false
		c.freezeL.Unlock()
	case cmdBalance:
		Info(c, " Balancing due to command")
		c.balance()
		Debug(c, " Finished balancing due to command")
	case cmdStopTask:
		taskI, ok := cmd.Parameters()["task"]
		task, ok2 := taskI.(string)
		if !ok || !ok2 {
			Error(c, " Stop task command didn't contain a valid task", c)
			return
		}
		Info("%s Stopping task %s due to command", c, task)
		c.stopTask(task)
	default:
		Warnf("%s Discarding unknown command: %s", c, cmd.Name())
	}
}

func (c *Consumer) ignored(taskID string) bool     { return c.im.ignored(taskID) }
func (c *Consumer) ignore(t Task, until time.Time) { c.im.add(t, until) }

// Ignores is a list of all ignored tasks.
func (c *Consumer) Ignores() []string { return c.im.all() }

func (c *Consumer) String() string {
	return c.coord.Name()
}
