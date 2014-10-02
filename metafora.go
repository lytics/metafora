package metafora

import (
	"log"
	"sort"
	"sync"
)

// ConsumerState is a limited interface exposed to Balancers for inspecting
// Consumer state.
type ConsumerState interface {
	// Tasks returns a sorted list of task IDs run by this Consumer. The Consumer
	// stops task manipulations during claiming and balancing, so the list will
	// be accurate unless a task naturally completes.
	Tasks() []string
}

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

	bal   Balancer
	coord Coordinator
}

// NewConsumer returns a new consumer and calls Init on the balancer.
func NewConsumer(coord Coordinator, h HandlerFunc, b Balancer) *Consumer {
	c := &Consumer{
		running: make(map[string]Handler),
		handler: h,
		bal:     b,
		coord:   coord,
	}
	b.Init(c)
	return c
}

func (c *Consumer) Start() {
	//TODO start etcd watches and call claimed for each one
}

func (c *Consumer) Shutdown() {
	log.Println("Sending stop signal to handlers")
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

	log.Println("Waiting for handlers to exit")
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
				log.Printf("Handler %s panic()'d: %v", taskID, err)
			}
			c.runL.Lock()
			delete(c.running, taskID)
			c.runL.Unlock()
			c.hwg.Done()
		}()

		if err := h.Run(taskID); err != nil {
			log.Printf("Handler for %s exited with error: %v", taskID, err)
		}
	}()
}

func (c *Consumer) release(taskID string) {
	//TODO release task ID
}
