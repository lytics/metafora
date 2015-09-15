package m_etcd

import (
	"encoding/json"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd/testutil"
)

type ctx struct{}

func (ctx) Lost(metafora.Task)                            {}
func (ctx) Log(metafora.LogLevel, string, ...interface{}) {}

type taskTest struct {
	Resp *etcd.Response
	Task mapTask
	Ok   bool
}

type mapTask map[string]interface{}

func (m mapTask) ID() string { return m["id"].(string) }

func TestParseTask(t *testing.T) {
	etcdc, _ := testutil.NewEtcdClient(t)
	t.Parallel()

	etcdc.Delete("test-parse", recursive)

	c := EtcdCoordinator{taskPath: "/test-parse/tasks", cordCtx: &ctx{}, NewTask: DefaultTaskFunc, client: etcdc}
	c.NewTask = func(id, value string) metafora.Task {
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

	// Unfortunately parseTasks sometimes has to go back out to etcd for
	// properties. Insert test data.
	etcdc.Create("/test-parse/tasks/0/props", "{invalid", foreverTTL)

	tests := []taskTest{
		// bad
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/test-parse/tasks/0/owner", Dir: false}}},
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/test-parse/oops/1", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/test-parse/tasks/1", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{Key: "/test-parse/tasks/1/a", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/test-parse/tasks/1", Dir: false}}},

		// good
		{
			Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{Key: "/test-parse/tasks/1", Dir: true}},
			Task: mapTask{"id": "1"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionSet, Node: &etcd.Node{Key: "/test-parse/tasks/2", Dir: true}},
			Task: mapTask{"id": "2"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/test-parse/tasks/3/owner"}},
			Task: mapTask{"id": "3"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionDelete, Node: &etcd.Node{Key: "/test-parse/tasks/4/owner"}},
			Task: mapTask{"id": "4"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{
				Key:   "/test-parse/tasks/5",
				Nodes: []*etcd.Node{{Key: "/test-parse/tasks/5/props", Value: `{"test": "ok"}`}},
				Dir:   true,
			}},
			Task: mapTask{"id": "5", "test": "ok"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionSet, Node: &etcd.Node{Key: "/test-parse/tasks/6/props", Value: `{"test":"ok"}`}},
			Task: mapTask{"id": "6", "test": "ok"},
			Ok:   true,
		},
	}

	for _, test := range tests {
		parsed := c.parseTask(test.Resp)
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
