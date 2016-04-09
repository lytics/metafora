package testutil

import (
	"log"
	"sync/atomic"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

var counter int64

type logger struct {
	c client.KeysAPI
}

// New logging wrapper around etcd's KeysAPI.
func NewLoggerAPI(c client.KeysAPI) client.KeysAPI {
	return &logger{c}
}

func (l *logger) Get(ctx context.Context, key string, opts *client.GetOptions) (*client.Response, error) {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - Get(%v, %q, %#v)", i, ctx, key, opts)
	r, e := l.c.Get(ctx, key, opts)
	var index uint64
	if r != nil {
		index = r.Index
	}
	nodes := 0
	if r != nil && r.Node != nil {
		nodes = len(r.Node.Nodes)
	}
	log.Printf("%5d - index=%d nodes=%d Get(...) -> (%#v, %v)", i, index, nodes, r, e)
	return r, e
}

func (l *logger) Set(ctx context.Context, key, value string, opts *client.SetOptions) (*client.Response, error) {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - Set(%v, %q, %q, %#v)", i, ctx, key, value, opts)
	r, e := l.c.Set(ctx, key, value, opts)
	var index uint64
	if r != nil {
		index = r.Index
	}
	log.Printf("%5d - index=%d Set(...) -> (%#v, %v)", i, index, r, e)
	if r != nil {
		log.Printf("%5d - index=%d Set(...) -> Node: %#v -- PrevNode: %#v", i, index, r.Node, r.PrevNode)
	}
	return r, e
}

func (l *logger) Delete(ctx context.Context, key string, opts *client.DeleteOptions) (*client.Response, error) {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - Delete(%v, %q, %#v)", i, ctx, key, opts)
	r, e := l.c.Delete(ctx, key, opts)
	var index uint64
	if r != nil {
		index = r.Index
	}
	log.Printf("%5d - index=%d Delete(...) -> (%#v, %v)", i, index, r, e)
	return r, e
}

func (l *logger) Create(ctx context.Context, key, value string) (*client.Response, error) {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - Create(%v, %q, %q)", i, ctx, key, value)
	r, e := l.c.Create(ctx, key, value)
	var index uint64
	if r != nil {
		index = r.Index
	}
	log.Printf("%5d - index=%d Create(...) -> (%#v, %v)", i, index, r, e)
	return r, e
}

func (l *logger) CreateInOrder(ctx context.Context, dir, value string, opts *client.CreateInOrderOptions) (*client.Response, error) {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - CreateInOrder(%v, %q, %q, %#v)", i, ctx, dir, value, opts)
	r, e := l.c.CreateInOrder(ctx, dir, value, opts)
	var index uint64
	if r != nil {
		index = r.Index
	}
	log.Printf("%5d - index=%d CreateInOrder(...) -> (%#v, %v)", i, index, r, e)
	return r, e
}

func (l *logger) Update(ctx context.Context, key, value string) (*client.Response, error) {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - Update(%v, %q, %q, %#v)", i, ctx, key, value)
	r, e := l.c.Update(ctx, key, value)
	var index uint64
	if r != nil {
		index = r.Index
	}
	log.Printf("%5d - index=%d Update(...) -> (%#v, %v)", i, index, r, e)
	return r, e
}

func (l *logger) Watcher(key string, opts *client.WatcherOptions) client.Watcher {
	i := atomic.AddInt64(&counter, 1)
	log.Printf("%5d - Watcher(%q, %#v)", i, key, opts)
	w := l.c.Watcher(key, opts)
	return &watcherlogger{i, w}
}

type watcherlogger struct {
	createdat int64
	w         client.Watcher
}

func (l *watcherlogger) Next(ctx context.Context) (*client.Response, error) {
	log.Printf("%5d - Next(%v)", l.createdat, ctx)
	r, e := l.w.Next(ctx)
	var index uint64
	if r != nil {
		index = r.Index
	}
	log.Printf("%5d - index=%d Next(...) -> (%#v, %v)", l.createdat, index, r, e)
	if r != nil {
		log.Printf("%5d - index=%d Next(...) -> Node: %#v", l.createdat, index, r.Node)
	}
	return r, e
}
