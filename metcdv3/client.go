package metcdv3

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"time"

	"github.com/lytics/metafora"
	etcdv3 "go.etcd.io/etcd/clientv3"
)

var (
	// ErrFailedSubmitTask because the task already existed most likely
	ErrFailedSubmitTask            = errors.New("metafora etcdv3 client: failed submit task")
	ErrLeaseDurationTooShort       = errors.New("metafora etcd clientv3: lease duration too short")
	ErrKeepAliveClosedUnexpectedly = errors.New("metafora etcd clientv3: keep alive closed unexpectedly")
)

var (
	minLeaseDuration = 10 * time.Second
)

// NewClient creates a new client using an etcd backend.
func NewClient(namespace string, etcdv3c *etcdv3.Client) metafora.Client {
	return &mclient{
		etcdv3c:   etcdv3c,
		kvc:       etcdv3.NewKV(etcdv3c),
		namespace: namespace,
	}
}

type keepAliveStats struct {
	success int
	failure int
}

// Type 'mclient' is an internal implementation of metafora.Client with an etcd backend.
type mclient struct {
	etcdv3c   *etcdv3.Client
	kvc       etcdv3.KV
	namespace string
}

// nodesPath is the base path of nodes, represented as a directory in etcd.
func (mc *mclient) nodesPath() string {
	return path.Join("/", mc.namespace, NodesPath)
}

// taskPath is the path to a particular taskId, represented as a file in etcd.
func (mc *mclient) taskPath(taskID string) string {
	return path.Join("/", mc.namespace, TasksPath, taskID)
}

// cmdPath is the path to a particular nodeId, represented as a directory in etcd.
func (mc *mclient) cmdPath(node string) string {
	return path.Join("/", mc.namespace, NodesPath, node, "commands")
}

// SubmitTask creates a new task in etcd
func (mc *mclient) SubmitTask(task metafora.Task) error {
	c := context.Background()
	fullpath := path.Join(mc.taskPath(task.ID()), PropsPath)
	buf, err := json.Marshal(task)
	if err != nil {
		return err
	}
	txnRes, err := mc.kvc.Txn(c).
		If(etcdv3.Compare(etcdv3.Version(fullpath), "=", 0)).
		// Should we create both of these?
		Then(etcdv3.OpPut(fullpath, string(buf)), etcdv3.OpPut(mc.taskPath(task.ID()), "")).
		Commit()
	if err != nil {
		return err
	}
	if !txnRes.Succeeded {
		return ErrFailedSubmitTask
	}
	metafora.Debugf("task %s submitted: %s", task.ID(), fullpath)
	return nil
}

// Delete a task
func (mc *mclient) DeleteTask(taskID string) error {
	c := context.Background()
	fullpath := mc.taskPath(taskID)
	_, err := mc.kvc.Delete(c, fullpath, etcdv3.WithPrefix())
	metafora.Debugf("task %s deleted: %s", taskID, fullpath)
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
	key := path.Join(cmdPath, strconv.FormatUint(rand.Uint64(), 10))
	if _, err := mc.kvc.Put(context.Background(), key, string(body)); err != nil {
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
	res, err := mc.kvc.Get(context.Background(), mc.nodesPath(), etcdv3.WithPrefix())
	if err != nil && res != nil && len(res.Kvs) > 0 {
		nodes := make([]string, len(res.Kvs))
		for i, kv := range res.Kvs {
			var node string
			err = json.Unmarshal(kv.Key, &node)
			if err != nil {
				return nil, err
			}
			nodes[i] = path.Base(node)
		}
		return nodes, nil
	}

	return nil, nil
}

func (mc *mclient) Tasks() ([]string, error) {
	res, err := mc.kvc.Get(
		context.Background(),
		path.Join("/", mc.namespace, TasksPath),
		etcdv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	var tasks []string
	for _, kv := range res.Kvs {
		key := string(kv.Key)
		if base := path.Base(key); base == OwnerPath || base == MetadataPath || base == PropsPath {
			continue
		} else {
			tasks = append(tasks, base)
		}
	}
	return tasks, nil
}
