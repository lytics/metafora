package metafora

import "log"

type Consumer struct {
	handler HandlerFunc
	running map[string]Handler
}

type Handler interface {
	Start(taskID string) error
	Stop() error
}

type HandlerFunc func(exit func()) Handler

func NewConsumer(etcdAddr string, h HandlerFunc) *Consumer {
	return &Consumer{
		handler: h,
	}
}

func (c *Consumer) claimed(taskID string) {

	// Create handler
	h := c.handler(func() { delete(c.running, taskID) })

	// Associate handler with taskID
	c.running[taskID] = h

	// Start handler in its own goroutine
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Handler %s panic()'d: %v", taskID, err)
			}
			delete(c.running, taskID)
		}()

		if err := h.Start(taskID); err != nil {
			log.Printf("Handler for %s exited with error: %v", taskID, err)
		}
	}()
}

func (c *Consumer) stop() {
	for id, h := range c.running {
		if err := h.Stop(); err != nil {
			log.Printf("Error from handler %s: %v", id, err)
		}
	}
}
