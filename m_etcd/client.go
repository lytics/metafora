package m_etcd

import (
	"fmt"
	"path"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type dumbLogger struct {
}

func (l dumbLogger) Log(lvl metafora.LogLevel, m string, v ...interface{}) {
	//NOOP
}

// NewClient creates a new client using an etcd backend.
func NewClient(namespace string, client *etcd.Client) metafora.Client {

	return &mclient{
		etcd:      client,
		namespace: namespace,
		logger:    &dumbLogger{},
	}
}

// NewClient creates a new client using an etcd backend.
func NewClientWithLogger(namespace string, client *etcd.Client, logger metafora.Logger) metafora.Client {
	namespace = strings.Trim(namespace, "/ ")
	return &mclient{
		etcd:      client,
		namespace: namespace,
		logger:    logger,
	}
}

// Type 'mclient' is an internal implementation of metafora.Client with an etcd backend.
type mclient struct {
	etcd      *etcd.Client
	namespace string
	logger    metafora.Logger
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

// SubmitTask creates a new taskId, represented as a directory in etcd.
func (mc *mclient) SubmitTask(taskId string) error {
	fullpath := mc.tskPath(taskId)
	_, err := mc.etcd.CreateDir(fullpath, ForeverTTL)
	mc.logger.Log(metafora.LogLevelDebug, "task submitted [%s]", fullpath)
	return err
}

// Delete a task
func (mc *mclient) DeleteTask(taskId string) error {
	fullpath := mc.tskPath(taskId)
	_, err := mc.etcd.DeleteDir(fullpath)
	mc.logger.Log(metafora.LogLevelDebug, "task deleted [%s]", fullpath)
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
	if _, err := mc.etcd.AddChild(cmdPath, string(body), ForeverTTL); err != nil {
		mc.logger.Log(metafora.LogLevelError, "Error submitting command: %s to node: %s", command, node)
		return err
	}
	mc.logger.Log(metafora.LogLevelDebug, "Submitted command: %s to node: %s", command, node)
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
