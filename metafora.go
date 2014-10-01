package metafora

import (
	"log"
	"sync"
)

type Consumer struct {
	handler HandlerFunc
	running map[string]Handler
	runL    sync.Mutex
	hwg     sync.WaitGroup
}

type Handler interface {
	// Run should block until a task is complete. If it returns nil, the task is
	// considered complete. If error is non-nil, ...well... log it? FIXME
	Run(taskID string) error

	// Stop should signal to the handler to shutdown gracefully. Stop
	// implementations should not block until Run exits.
	Stop()
}

type HandlerFunc func() Handler

func NewConsumer(etcdAddr string, h HandlerFunc) *Consumer {
	return &Consumer{
		running: make(map[string]Handler),
		handler: h,
	}
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

func (c *Consumer) claimed(taskID string) {

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
