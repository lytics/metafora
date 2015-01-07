package m_etcd

import (
	"errors"
	"net"
	"net/http"
	"time"
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
