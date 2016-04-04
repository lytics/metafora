package m_etcd

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
)

type Config struct {
	// Namespace is the key prefix to allow for multitenant use of etcd.
	//
	// Namespaces must start with a / (added by NewConfig if needed).
	Namespace string

	// Name of this Metafora consumer. Only one instance of a Name is allowed to
	// run in a Namespace at a time, so if you set the Name to hostname you can
	// effectively limit Metafora to one process per server.
	Name string

	// ClaimTTL is the timeout on task claim markers in seconds.
	//
	// Since every task must update its claim before the TTL expires, setting
	// this lower will increase the load on etcd. Setting this setting higher
	// increases the amount of time it takes a task to be rescheduled if the node
	// it was running on shutsdown uncleanly (or is separated by a network
	// partition).
	//
	// If 0 it is set to DefaultClaimTTL
	ClaimTTL time.Duration

	// NodeTTL is the timeout on the node's name entry in seconds.
	//
	// If 0 it is set to DefaultNodeTTL
	NodeTTL time.Duration

	// NewTaskFunc is the function called to unmarshal tasks from etcd into a
	// custom struct. The struct must implement the metafora.Task interface.
	//
	// If nil it is set to DefaultTaskFunc
	NewTaskFunc TaskFunc

	// EtcdConfig will be used to configure the etcd client.
	EtcdConfig client.Config
}

// NewConfig creates a Config with the required fields and uses defaults for
// the others. hosts should be specified as fully qualified URLs.
//
// Panics on empty values.
func NewConfig(name, namespace string, hosts []string) *Config {
	if len(hosts) == 0 || namespace == "" || name == "" {
		panic("invalid etcd config")
	}

	namespace = "/" + strings.Trim(namespace, "/ ")

	return &Config{
		Name:        name,
		Namespace:   namespace,
		ClaimTTL:    DefaultClaimTTL,
		NodeTTL:     DefaultNodeTTL,
		NewTaskFunc: DefaultTaskFunc,
		EtcdConfig: client.Config{
			Endpoints:               hosts,
			HeaderTimeoutPerRequest: 5 * time.Second,
		},
	}
}

// Copy returns a shallow copy of this config.
func (c *Config) Copy() *Config {
	return &Config{
		Name:        c.Name,
		Namespace:   c.Namespace,
		ClaimTTL:    c.ClaimTTL,
		NodeTTL:     c.NodeTTL,
		NewTaskFunc: c.NewTaskFunc,
		EtcdConfig:  c.EtcdConfig,
	}
}

func (c *Config) String() string {
	return fmt.Sprintf("etcd:%s/%s", c.Namespace, c.Name)
}

// Validate this configuration. Returns an error if the congiuration is
// invalid.
func (c *Config) Validate() error {
	if c.ClaimTTL <= time.Second {
		return fmt.Errorf("ClaimTTL must be > 1s")
	}
	if c.NodeTTL <= time.Second {
		return fmt.Errorf("NodeTTL must be > 1s")
	}
	if len(c.EtcdConfig.Endpoints) == 0 {
		return fmt.Errorf("EtcdConfig.Endpoints missing.")
	}
	if c.Name == "" {
		return fmt.Errorf("Name cannot be empty")
	}
	return nil
}
