package metafora

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

const balanceJitterMax = 10 * int64(time.Second)

var (
	random = rand.New(rand.NewSource(time.Now().UnixNano()))

	//FIXME should probably be improved, see usage in Run()
	consumerRetryDelay = 10 * time.Second
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

	bal      Balancer
	balEvery time.Duration
	coord    Coordinator
	logger   Logger
	stop     chan struct{} // closed by Shutdown to cause Run to exit
}

// NewConsumer returns a new consumer and calls Init on the Balancer and Coordinator.
func NewConsumer(coord Coordinator, h HandlerFunc, b Balancer) *Consumer {
	c := &Consumer{
		running:  make(map[string]Handler),
		handler:  h,
		bal:      b,
		balEvery: 15 * time.Minute, //TODO make balance wait configurable
		coord:    coord,
		logger:   NewBasicLogger(),
		stop:     make(chan struct{}),
	}

	// initialize balancer with the consumer and a prefixed logger
	b.Init(&struct {
		*Consumer
		Logger
	}{Consumer: c, Logger: NewPrefixLogger(c.logger, "balancer:")})

	// initialize coordinator with a logger
	coord.Init(NewPrefixLogger(c.logger, "coordinator:"))
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

// Run is the core run loop of Metafora. It is responsible for calling into the
// Coordinator to claim work and Balancer to rebalance work.
//
// Run blocks until Shutdown is called.
func (c *Consumer) Run() {
	// Call Balance in a goroutine
	go func() {
		for {
			select {
			case <-c.stop:
				// Shutdown has been called.
				return
			case <-time.After(c.balEvery + time.Duration(random.Int63n(balanceJitterMax))):
				c.logger.Log(LogLevelDebug, "Balancing")
				c.bal.Balance()
			}
		}
	}()

	watch := make(chan string)
	cmdChan := make(chan string)
	cont := make(chan struct{})

	// Watch for new tasks in a goroutine
	go func() {
		for {
			task, err := c.coord.Watch()
			if err != nil {
				//FIXME add more sophisticated error handling
				c.logger.Log(LogLevelError, "Coordinator returned an error during watch, waiting and retrying. %v", err)
				select {
				case <-c.stop:
					return
				case <-time.After(consumerRetryDelay):
				}
				continue
			}
			// Send task to watcher (or shutdown)
			select {
			case <-c.stop:
				return
			case watch <- task:
			}
			// Wait for watcher to acknowledge task before continuing (or shutdown)
			select {
			case <-c.stop:
				return
			case <-cont:
			}
		}
	}()

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
			// Send command to watcher (or shutdown)
			select {
			case <-c.stop:
				return
			case cmdChan <- cmd:
			}
			// Wait for watcher to acknowledge command before continuing (or shutdown)
			select {
			case <-c.stop:
				return
			case <-cont:
			}
		}
	}()

	// Main Loop
	for {
		select {
		case <-c.stop:
			// Shutdown has been called.
			return
		case task := <-watch:
			if !c.bal.CanClaim(task) {
				c.logger.Log(LogLevelInfo, "Balancer rejected task %s", task)
			}
			if !c.coord.Claim(task) {
				c.logger.Log(LogLevelInfo, "Coordinator unable to claim task %s", task)
			}
			c.claimed(task)
			cont <- struct{}{} // signal watch to continue
		case cmd := <-cmdChan:
			//FIXME Handle commands
			c.logger.Log(LogLevelWarning, "Received command: %s", cmd)
		}
	}
}

// Shutdown stops the main Run loop, stops all handlers, and releases their
// tasks.
func (c *Consumer) Shutdown() {
	c.logger.Log(LogLevelDebug, "Signalling shutdown")
	close(c.stop)
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
