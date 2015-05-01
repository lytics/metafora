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

	"github.com/coreos/go-etcd/etcd"
)

// TestCase just defines the subset of *testing.T methods needed to avoid
// pulling in the testing package.
type TestCase interface {
	Skip(args ...interface{})
	Fatalf(format string, args ...interface{})
}

// NewEtcdClient creates a new etcd client for use by the metafora client during testing.
func NewEtcdClient(t TestCase) *etcd.Client {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}

	// This is the same ENV variable that etcdctl uses for peers.
	peerAddrs := os.Getenv("ETCD_PEERS")

	if peerAddrs == "" {
		peerAddrs = "127.0.0.1:4001"
	}

	peers := strings.Split(peerAddrs, ",")

	eclient := etcd.NewClient(peers)

	if ok := eclient.SyncCluster(); !ok {
		t.Fatalf("Cannot sync etcd cluster using peers: %v", strings.Join(peers, ", "))
	}

	eclient.SetConsistency(etcd.STRONG_CONSISTENCY)

	return eclient
}
