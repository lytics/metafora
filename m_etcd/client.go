package m_etcd

import (
	"encoding/json"
	"fmt"
	"path"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
)

// NewClient creates a new client using an etcd backend.
func NewClient(namespace string, hosts []string) metafora.Client {
	return NewClientFromConfig(NewConfig("_client", namespace, hosts))
}

// NewClientFromConfig creates a new client using an etcd backend from an etcd
// coordinator configuration.
//
// Coordinator specific configuration values such as Name and TTLs are ignored. This is a conve
func NewClientFromConfig(conf *Config) metafora.Client {
	client, _ := newEtcdClient(conf)
	return &mclient{
		etcd:      client,
		namespace: conf.Namespace,
	}
}

// Type 'mclient' is an internal implementation of metafora.Client with an etcd backend.
type mclient struct {
	etcd      client.KeysAPI
	namespace string
}

// ndsPath is the base path of nodes, represented as a directory in etcd.
func (mc *mclient) ndsPath() string {
	return fmt.Sprintf("/%s/%s", mc.namespace, NodesPath)
}

// tskPath is the path to a particular taskId, represented as a file in etcd.
func (mc *mclient) tskPath(taskId string) string {
	return fmt.Sprintf("/%s/%s/%s", mc.namespace, TasksPath, taskId)
}

// cmdPath is the path to a particular nodeId, represented as a directory in etcd.
func (mc *mclient) cmdPath(node string) string {
	return path.Join("/", mc.namespace, NodesPath, node, "commands")
}

// SubmitTask creates a new task in etcd
func (mc *mclient) SubmitTask(task metafora.Task) error {
	fullpath := path.Join(mc.tskPath(task.ID()), PropsKey)
	buf, err := json.Marshal(task)
	if err != nil {
		return err
	}
	if _, err := mc.etcd.Create(context.TODO(), fullpath, string(buf)); err != nil {
		return err
	}
	metafora.Debugf("task %s submitted: %s", task.ID(), fullpath)
	return nil
}

// DeleteTask from etcd. This will not signal for a running task to stop. See
// the statemachine package for sending commands to tasks.
//
// This method should be used rarely outside of tests.
func (mc *mclient) DeleteTask(taskId string) error {
	fullpath := mc.tskPath(taskId)
	if _, err := mc.etcd.Delete(context.TODO(), fullpath, delRecurDir); err != nil {
		return err
	}
	metafora.Debugf("task %s deleted: %s", taskId, fullpath)
	return nil
}

// SubmitCommand creates a new command for a particular nodeId, the
// command has a random name and is added to the particular nodeId
// directory in etcd.
func (mc *mclient) SubmitCommand(node string, command metafora.Command) error {
	cmdPath := mc.cmdPath(node)
	body, err := command.Marshal()
	if err != nil {
		// This is either a bug in metafora or someone implemented their own
		// command incorrectly.
		return err
	}
	if _, err := mc.etcd.CreateInOrder(context.TODO(), cmdPath, string(body), createOrdered); err != nil {
		metafora.Errorf("Error submitting command: %s to node: %s", command, node)
		return err
	}
	metafora.Debugf("Submitted command: %s to node: %s", string(body), node)
	return nil
}

// Nodes fetchs the currently registered nodes. A non-nil error means that some
// error occured trying to get the node list. The node list may be nil if no
// nodes are registered.
func (mc *mclient) Nodes() ([]string, error) {
	res, err := mc.etcd.Get(context.TODO(), mc.ndsPath(), getSortRecur)
	if err != nil {
		return nil, err
	}
	nodes := make([]string, len(res.Node.Nodes))
	for i, n := range res.Node.Nodes {
		_, nodes[i] = path.Split(n.Key)
	}
	return nodes, nil
}
