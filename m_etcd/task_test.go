package m_etcd_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
	"github.com/lytics/metafora/m_etcd/testutil"
	"github.com/lytics/metafora/statemachine"
)

// exTask is an extended Task type to demonstrate using an alternative NewTask
// TaskFunc.
type exTask struct {
	id         string
	SubmittedT *time.Time `json:"_submitted"`
	UserID     string     `json:userid`
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
	etcdc, hosts := testutil.NewEtcdClient(t)
	t.Parallel()
	const namespace = "alttask-metafora"

	etcdc.Delete(namespace, recursive)

	conf := m_etcd.NewConfig("testclient", namespace, hosts)

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

	coord, hf, bal, err := m_etcd.New(conf, h)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := metafora.NewConsumer(coord, hf, bal)
	if err != nil {
		t.Fatal(err)
	}
	go consumer.Run()
	defer consumer.Shutdown()

	cli := m_etcd.NewClient(namespace, hosts)
	if err := cli.SubmitTask(&exTask{id: "test1", UserID: "test2"}); err != nil {
		t.Fatal(err)
	}

	result := <-results
	if result != "ok" {
		t.Fatal(result)
	}
}
