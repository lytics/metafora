// Package testutil is a collection of utilities for use by Metafora's etcd
// tests. Since tests are spread across the m_etcd and m_etcd_test packages
// utilities must be in a shared location.
//
// Unless you're making changes to the m_etcd package you don't need to use
// this.
package testutil

import (
	"os"
	"strings"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

// TestCase just defines the subset of *testing.T methods needed to avoid
// pulling in the testing package.
type TestCase interface {
	Skip(args ...interface{})
	Fatalf(format string, args ...interface{})
}

// NewEtcdV3Client creates a new etcd client for use by the metafora client during testing.
func NewEtcdV3Client(t TestCase) *etcdv3.Client {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}

	// This is the same ENV variable that etcdctl uses for peers.
	peerAddrs := os.Getenv("ETCD_PEERS")
	if peerAddrs == "" {
		peerAddrs = "127.0.0.1:2379"
	}

	peers := strings.Split(peerAddrs, ",")
	cli, err := etcdv3.New(etcdv3.Config{
		Endpoints:   peers,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create etcdv3 client: %v", err)
	}
	//defer cli.Close()
	return cli
}
