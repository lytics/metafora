package m_etcd

import (
	"fmt"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

func NewEtcdClient(namespace string, client *etcd.Client) metafora.Client {
	taskPath := fmt.Sprintf("/%s/%s", namespace, TASKS_PATH) //TODO MAKE A PACKAGE FUNC TO CREATE THIS PATH.
	return &mClient{
		etcd:     client,
		taskPath: taskPath,
	}
}

type mClient struct {
	etcd     *etcd.Client
	taskPath string
}

func (mc *mClient) SubmitTask(taskId string) error {
	task := fmt.Sprintf("%s/%s", mc.taskPath, taskId)
	_, err := mc.etcd.CreateDir(task, TTL_FOREVER)

	return err
}
