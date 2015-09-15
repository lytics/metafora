package m_etcd

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Config struct {
	// Namespace is the key prefix to allow for multitenant use of etcd.
	Namespace string

	// Name of this Metafora consumer. Only one instance of a Name is allowed to
	// run in a Namespace at a time, so if you set the Name to hostname you can
	// effectively limit Metafora to one process per server.
	Name string

	// Hosts are the URLs to create etcd clients with.
	Hosts []string

	// ClaimTTL is the timeout on task claim markers in seconds.
	//
	// Since every task must update its claim before the TTL expires, setting
	// this lower will increase the load on etcd. Setting this setting higher
	// increases the amount of time it takes a task to be rescheduled if the node
	// it was running on shutsdown uncleanly (or is separated by a network
	// partition).
	//
	// If 0 it is set to DefaultClaimTTL
	ClaimTTL uint64

	// NodeTTL is the timeout on the node's name entry in seconds.
	//
	// If 0 it is set to DefaultNodeTTL
	NodeTTL uint64

	// NewTaskFunc is the function called to unmarshal tasks from etcd into a
	// custom struct. The struct must implement the metafora.Task interface.
	//
	// If nil it is set to DefaultTaskFunc
	NewTaskFunc TaskFunc
}

// NewConfig creates a Config with the required fields and uses defaults for
// the others.
//
// Panics on empty values.
func NewConfig(namespace string, hosts []string) *Config {
	if len(hosts) == 0 || namespace == "" {
		panic("invalid etcd config")
	}

	namespace = "/" + strings.Trim(namespace, "/ ")

	hn, err := os.Hostname()
	if err != nil {
		panic("error getting hostname: " + err.Error())
	}
	name := fmt.Sprintf("%s-%x", hn, rand.New(rand.NewSource(time.Now().UnixNano())).Int63())

	return &Config{
		Name:        name,
		Namespace:   namespace,
		Hosts:       hosts,
		ClaimTTL:    DefaultClaimTTL,
		NodeTTL:     DefaultNodeTTL,
		NewTaskFunc: DefaultTaskFunc,
	}
}

// Copy returns a shallow copy of this config.
func (c *Config) Copy() *Config {
	return &Config{
		Name:        c.Name,
		Namespace:   c.Namespace,
		Hosts:       c.Hosts,
		ClaimTTL:    c.ClaimTTL,
		NodeTTL:     c.NodeTTL,
		NewTaskFunc: c.NewTaskFunc,
	}
}

func (c *Config) String() string {
	return fmt.Sprintf("etcd:%s/%s", c.Namespace, c.Name)
}
