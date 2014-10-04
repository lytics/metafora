package m_etcd

import (
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

// Create a new client using an etcd backend.
func NewClient(namespace string, client *etcd.Client) metafora.Client {
	return &mclient{
		etcd:      client,
		namespace: namespace,
	}
}

// Internal implementation of metafora.Client with an etcd backend.
type mclient struct {
	etcd      *etcd.Client
	namespace string
}

// Path to base directory of nodes, which is a directory in etcd.
func (mc *mclient) ndsPath() string {
	return fmt.Sprintf("/%s/%s", mc.namespace, NODES_PATH)
}

// Path to particular taskId, which is a file in etcd.
func (mc *mclient) tskPath(taskId string) string {
	return fmt.Sprintf("/%s/%s/%s", mc.namespace, TASKS_PATH, taskId)
}

// Path to particular nodeId, which is a directory in etcd.
func (mc *mclient) cmdPath(nodeId string) string {
	return fmt.Sprintf("/%s/%s/%s", mc.namespace, NODES_PATH, nodeId)
}

// Create a new taskId, the taskId is a directory in etcd.
func (mc *mclient) SubmitTask(taskId string) error {
	_, err := mc.etcd.CreateDir(mc.tskPath(taskId), TTL_FOREVER)

	return err
}

// Create a new command for a particular nodeId, the command has a random
// name and is added to the particular nodeId directory in etcd.
func (mc *mclient) SubmitCommand(nodeId string, command string) error {
	_, err := mc.etcd.AddChild(mc.cmdPath(nodeId), command, TTL_FOREVER)

	return err
}

// Fetch the current nodes. A non-nil error means that some error occured
// trying to get the node list. The node list may be nil if no nodes are
// registered.
func (mc *mclient) Nodes() ([]string, error) {
	if res, err := mc.etcd.Get(mc.ndsPath(), false, false); err != nil {
		return make([]string, 0), err
	} else if res.Node != nil && res.Node.Nodes != nil {
		nodes := make([]string, len(res.Node.Nodes))
		for i, n := range res.Node.Nodes {
			nodes[i] = n.Value
		}
		return nodes, nil
	} else {
		return nil, nil
	}
}
