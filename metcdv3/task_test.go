package metcdv3_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/metcdv3"
	"github.com/lytics/metafora/metcdv3/testutil"
	"github.com/lytics/metafora/statemachine"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

// exTask is an extended Task type to demonstrate using an alternative NewTask
// TaskFunc.
type exTask struct {
	id         string
	SubmittedT *time.Time `json:"_submitted"`
	UserID     string     `json:"UserID"`
}

func (t *exTask) ID() string            { return t.id }
func (t *exTask) Submitted() *time.Time { return t.SubmittedT }
func (t *exTask) String() string {
	if t.SubmittedT == nil {
		return t.id
	}
	return fmt.Sprintf("%s submitted %s", t.id, t.SubmittedT)
}

func TestAltTask(t *testing.T) {
	etcdv3c := testutil.NewEtcdV3Client(t)
	kvc := etcdv3.NewKV(etcdv3c)
	c := context.Background()
	t.Parallel()
	const namespace = "/alttask-metafora"
	_, _ = kvc.Delete(c, namespace, etcdv3.WithPrefix())

	conf := metcdv3.NewConfig("testclient", namespace)

	// Sample overridden NewTask func
	conf.NewTaskFunc = func(id, props string) metafora.Task {
		task := exTask{id: id}
		if err := json.Unmarshal([]byte(props), &task); err != nil {
			metafora.Warnf("%s properties could not be unmarshalled: %v", id, err)
		}
		return &task
	}

	// Create a handler that returns results through a chan for synchronization
	results := make(chan string, 1)

	h := func(task metafora.Task, _ <-chan *statemachine.Message) *statemachine.Message {
		alttask, ok := task.(*exTask)
		if !ok {
			results <- fmt.Sprintf("%q is of type %T", task.ID(), task)
			return statemachine.PauseMessage()
		}
		if alttask.UserID == "" {
			results <- "missing UserID"
			return statemachine.PauseMessage()
		}
		results <- "ok"
		return statemachine.PauseMessage()
	}

	coord, hf, bal := metcdv3.New(conf, etcdv3c, h)
	consumer, err := metafora.NewConsumer(coord, hf, bal)
	if err != nil {
		t.Fatal(err)
	}
	go consumer.Run()
	defer consumer.Shutdown()

	cli := metcdv3.NewClient(namespace, etcdv3c)
	if err := cli.SubmitTask(&exTask{id: "test1", UserID: "test2"}); err != nil {
		t.Fatal(err)
	}

	result := <-results
	if result != "ok" {
		t.Fatal(result)
	}
}
