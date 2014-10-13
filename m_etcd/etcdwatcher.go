package m_etcd

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type Watcher func(cordCtx metafora.CoordinatorContext)
type EtcdWatcher struct {
	cordCtx      metafora.CoordinatorContext
	path         string
	responseChan chan *etcd.Response
	stopChan     chan bool
	errorChan    chan error
	client       *etcd.Client
	isClosed     bool
}

func (w *EtcdWatcher) StartWatching() {
	go func() {
		const recursive = true
		const etcdIndex = uint64(0) //0 == latest version

		//TODO, Im guessing that watch only picks up new nodes.
		//   We need to manually do an ls at startup and dump the results to taskWatcherResponses,
		//   after we filter out all non-claimed tasks.
		_, err := w.client.Watch(
			w.path,
			etcdIndex,
			recursive,
			w.responseChan,
			w.stopChan)
		if err == etcd.ErrWatchStoppedByUser {
			// This isn't actually an error, return nil
			err = nil
		}
		w.errorChan <- err
		close(w.errorChan)
	}()
}

// Close stops the watching goroutine. Close will panic if called more than
// once.
func (w *EtcdWatcher) Close() error {
	if w.isClosed {
		close(w.stopChan)
		w.isClosed = true
		return <-w.errorChan
	}

	return nil
}
