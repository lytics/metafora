package m_etcd

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

// etcd has a bug where Watch() dies every 5 minutes and needs restarting --
// but you need to clear idle connections before restarting.
var transport = &http.Transport{Dial: dial}

// dial is copied from go-etcd's Client.dial method
func dial(network, addr string) (net.Conn, error) {
	//FIXME Don't use a constant for timeout
	conn, err := net.DialTimeout(network, addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, errors.New("Failed type-assertion of net.Conn as *net.TCPConn")
	}

	// Keep TCP alive to check whether or not the remote machine is down
	if err = tcpConn.SetKeepAlive(true); err != nil {
		return nil, err
	}

	if err = tcpConn.SetKeepAlivePeriod(time.Second); err != nil {
		return nil, err
	}

	return tcpConn, nil
}

func ispanic(err error) bool {
	_, ok := err.(*panicerror)
	return ok
}

type panicerror struct {
	error
}

// protectedRawWatch wraps watch in a panic recovery to work around https://github.com/coreos/go-etcd/pull/212
func protectedRawWatch(client *etcd.Client, path string, index uint64, recursive bool, receiver chan *etcd.RawResponse, stop chan bool) (resp *etcd.RawResponse, err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			perr, ok := rerr.(error)
			if ok {
				err = &panicerror{perr}
			} else {
				err = fmt.Errorf("Unknown recover error: %v", rerr)
			}
		}
	}()

	return client.RawWatch(path, index, recursive, receiver, stop)
}

// newEtcdClient is a simple helper to create a new etcd client with the custom
// transport and strong consistency level.
func newEtcdClient(hosts []string) (*etcd.Client, error) {
	c := etcd.NewClient(hosts)
	c.SetTransport(transport)
	if err := c.SetConsistency(etcd.STRONG_CONSISTENCY); err != nil {
		return nil, err
	}
	if !c.SyncCluster() {
		return nil, fmt.Errorf("Unable to communicate with etcd cluster %q", strings.Join(hosts, ","))
	}
	return c, nil
}
