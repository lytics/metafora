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

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

// TestCase just defines the subset of *testing.T methods needed to avoid
// pulling in the testing package.
type TestCase interface {
	Skip(args ...interface{})
	Fatalf(format string, args ...interface{})
	Logf(string, ...interface{})
}

// NewEtcdClient creates a new etcd client for use by the metafora client during testing.
func NewEtcdClient(t TestCase) (client.KeysAPI, client.Config) {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}

	// This is the same ENV variable that etcdctl uses for peers.
	peerAddrs := os.Getenv("ETCD_PEERS")

	if peerAddrs == "" {
		peerAddrs = "http://127.0.0.1:2379"
	}

	peers := strings.Split(peerAddrs, ",")

	conf := client.Config{
		Endpoints:               peers,
		HeaderTimeoutPerRequest: 10 * time.Second,
	}
	c, err := client.New(conf)
	if err != nil {
		t.Fatalf("Error creating etcd client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Sync(ctx); err != nil {
		t.Fatalf("Cannot sync etcd cluster using peers: %v: %v", strings.Join(peers, ", "), err)
	}

	return client.NewKeysAPI(c), conf
}

var cleanupopts = &client.DeleteOptions{Recursive: true, Dir: true}

func Cleanup(t TestCase, c client.KeysAPI, namespace string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Delete(ctx, "/"+namespace, cleanupopts); err != nil {
		t.Logf("Error deleting namespace %q - %v", namespace, err)
		return err
	}
	return nil
}
