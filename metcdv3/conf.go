package metcdv3

import (
	"fmt"
	"path"
	"strings"
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

	// Hosts are the URLs to create etcd clients with.
	Hosts []string

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
func NewConfig(name, namespace string, hosts []string) *Config {
	if len(hosts) == 0 || namespace == "" || name == "" {
		panic("invalid etcd config")
	}

	namespace = path.Join("/", strings.Trim(namespace, "/ "))
	return &Config{
		Name:        name,
		Namespace:   namespace,
		Hosts:       hosts,
		NewTaskFunc: DefaultTaskFunc,
	}
}

// Copy returns a shallow copy of this config.
func (c *Config) Copy() *Config {
	return &Config{
		Name:        c.Name,
		Namespace:   c.Namespace,
		Hosts:       c.Hosts,
		NewTaskFunc: c.NewTaskFunc,
	}
}

func (c *Config) String() string {
	return fmt.Sprintf("etcd:%s/%s", c.Namespace, c.Name)
}
