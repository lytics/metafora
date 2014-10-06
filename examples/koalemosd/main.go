package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
)

func main() {
	peers := flag.String("etcd", "127.0.0.1:5001", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	name := flag.String("name", "", "node name or empty for automatic")
	flag.Parse()

	etcdc := etcd.NewClient(strings.Split(*peers, ","))

	if !etcdc.SyncCluster() {
		log.Fatalf("Unable to connect to etcd cluster: %s", *peers)
	}

	hfunc := makeHandlerFunc(etcdc)
	coord := m_etcd.NewEtcdCoordinator(*name, *namespace, etcdc).(*m_etcd.EtcdCoordinator)
	bal := &metafora.DumbBalancer{}
	c := metafora.NewConsumer(coord, hfunc, bal)
	log.Printf("Starting koalsmosd with etcd=%s; namespace=%s; name=%s", *peers, *namespace, coord.NodeID)
	go c.Run()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, os.Kill)
	select {
	case s := <-sigC:
		log.Printf("Received signal %s, shutting down", s)
	}
	c.Shutdown()
	log.Printf("Shutdown")
}
