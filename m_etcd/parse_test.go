package m_etcd

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type ctx struct{}

func (ctx) Lost(string)                                   {}
func (ctx) Log(metafora.LogLevel, string, ...interface{}) {}

type taskTest struct {
	Resp *etcd.Response
	Task string
	Ok   bool
}

func TestParseTask(t *testing.T) {
	c := EtcdCoordinator{taskPath: "/namespace/tasks", cordCtx: &ctx{}}

	tests := []taskTest{
		// bad
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/1", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{Key: "/namespace/tasks/1/a", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/1", Dir: false}}},

		// good
		{
			Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{Key: "/namespace/tasks/1", Dir: true}},
			Task: "1",
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionSet, Node: &etcd.Node{Key: "/namespace/tasks/1", Dir: true}},
			Task: "1",
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/1/owner"}},
			Task: "1",
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionDelete, Node: &etcd.Node{Key: "/namespace/tasks/1/owner"}},
			Task: "1",
			Ok:   true,
		},
	}

	for _, test := range tests {
		task, ok := c.parseTask(test.Resp)
		if task != test.Task {
			t.Errorf("Test %s:%v failed: %s != %s", test.Resp.Action, *test.Resp.Node, task, test.Task)
		}
		if ok != test.Ok {
			t.Errorf("Test %s:%v failed: %v != %v", test.Resp.Action, *test.Resp.Node, ok, test.Ok)
		}
	}
}
