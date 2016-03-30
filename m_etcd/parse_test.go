package m_etcd

import (
	"encoding/json"
	"path"
	"testing"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
	"github.com/lytics/metafora"
)

type ctx struct{}

func (ctx) Lost(metafora.Task)                            {}
func (ctx) Log(metafora.LogLevel, string, ...interface{}) {}

type taskTest struct {
	Resp *client.Response
	Task mapTask
	Ok   bool
}

type mapTask map[string]interface{}

func (m mapTask) ID() string { return m["id"].(string) }

func TestParseTask(t *testing.T) {
	t.Parallel()
	ctx := setupEtcd(t)
	defer ctx.Cleanup()

	ctx.Conf.NewTaskFunc = func(id, value string) metafora.Task {
		tsk := mapTask{"id": id}
		if value == "" {
			return tsk
		}
		if err := json.Unmarshal([]byte(value), &tsk); err != nil {
			metafora.Warnf("Failed to unmarshal %q: %v", value, err)
			return nil
		}
		return tsk
	}

	taskp := func(rest string) string { return path.Join(ctx.Conf.Namespace, TasksPath, rest) }

	// Unfortunately parseTasks sometimes has to go back out to etcd for
	// properties. Insert test data.
	ctx.EtcdClient.Create(context.TODO(), taskp("/0/props"), "{invalid")

	tests := []taskTest{
		// bad
		{Resp: &client.Response{Action: actionCAD, Node: &client.Node{Key: taskp("/0/owner"), Dir: false}}},
		{Resp: &client.Response{Action: actionCAD, Node: &client.Node{Key: ctx.Conf.Namespace + "/oops/1", Dir: true}}},
		{Resp: &client.Response{Action: actionCAD, Node: &client.Node{Key: taskp("/1"), Dir: true}}},
		{Resp: &client.Response{Action: actionCreated, Node: &client.Node{Key: taskp("/1/a"), Dir: true}}},
		{Resp: &client.Response{Action: actionCAD, Node: &client.Node{Key: taskp("/1"), Dir: false}}},

		// good
		{
			Resp: &client.Response{Action: actionCreated, Node: &client.Node{Key: taskp("/1"), Dir: true}},
			Task: mapTask{"id": "1"},
			Ok:   true,
		},
		{
			Resp: &client.Response{Action: actionSet, Node: &client.Node{Key: taskp("/2"), Dir: true}},
			Task: mapTask{"id": "2"},
			Ok:   true,
		},
		{
			Resp: &client.Response{Action: actionCAD, Node: &client.Node{Key: taskp("/3/owner")}},
			Task: mapTask{"id": "3"},
			Ok:   true,
		},
		{
			Resp: &client.Response{Action: actionDelete, Node: &client.Node{Key: taskp("/4/owner")}},
			Task: mapTask{"id": "4"},
			Ok:   true,
		},
		{
			Resp: &client.Response{Action: actionCreated, Node: &client.Node{
				Key:   taskp("/5"),
				Nodes: []*client.Node{{Key: taskp("/5/props"), Value: `{"test": "ok"}`}},
				Dir:   true,
			}},
			Task: mapTask{"id": "5", "test": "ok"},
			Ok:   true,
		},
		{
			Resp: &client.Response{Action: actionSet, Node: &client.Node{Key: taskp("/6/props"), Value: `{"test":"ok"}`}},
			Task: mapTask{"id": "6", "test": "ok"},
			Ok:   true,
		},
	}

	for _, test := range tests {
		parsed := ctx.Coord.parseTask(test.Resp)
		if parsed == nil {
			if test.Ok {
				t.Errorf("Test %s:%v failed: expected task: %s", test.Resp.Action, *test.Resp.Node, test.Task)
			}
			continue
		}
		if !test.Ok {
			t.Errorf("Test %s:%v should have failed, but did not!", test.Resp.Action, *test.Resp.Node)
			continue
		}
		mt, ok := parsed.(mapTask)
		if !ok {
			t.Errorf("Test %s:%v didn't return a mapTask: %T", test.Resp.Action, *test.Resp.Node, parsed)
		}
		for k, v := range test.Task {
			if mt[k] != v {
				t.Errorf("Test %s:%v failed: %#v != %#v", test.Resp.Action, *test.Resp.Node, mt, test.Task)
			}
		}
	}
}
