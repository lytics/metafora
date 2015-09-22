package m_etcd

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

// NewClient creates a new client using an etcd backend.
func NewClient(namespace string, hosts []string) metafora.Client {
	client, _ := newEtcdClient(hosts)
	return &mclient{
		etcd:      client,
		namespace: namespace,
	}
}

// Type 'mclient' is an internal implementation of metafora.Client with an etcd backend.
type mclient struct {
	etcd      *etcd.Client
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
	if _, err := mc.etcd.Create(fullpath, string(buf), foreverTTL); err != nil {
		return err
	}
	metafora.Debugf("task %s submitted: %s", task.ID(), fullpath)
	return nil
}

// Delete a task
func (mc *mclient) DeleteTask(taskId string) error {
	fullpath := mc.tskPath(taskId)
	_, err := mc.etcd.Delete(fullpath, recursive)
	metafora.Debugf("task %s deleted: %s", taskId, fullpath)
	return err
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
	if _, err := mc.etcd.AddChild(cmdPath, string(body), foreverTTL); err != nil {
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
	if res, err := mc.etcd.Get(mc.ndsPath(), false, false); err != nil && res.Node != nil && res.Node.Nodes != nil {
		nodes := make([]string, len(res.Node.Nodes))
		for i, n := range res.Node.Nodes {
			nodes[i] = n.Value
		}
		return nodes, nil
	}

	return nil, nil
}
