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

const lflags = log.Ldate | log.Lmicroseconds | log.Lshortfile

var logger = log.New(os.Stdout, "", lflags)

func main() {
	mlvl := metafora.LogLevelInfo

	peers := flag.String("etcd", "127.0.0.1:5001", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	name := flag.String("name", "", "node name or empty for automatic")
	loglvl := flag.String("log", mlvl.String(), "set log level: [debug], info, warn, error")
	flag.Parse()

	etcdc := etcd.NewClient(strings.Split(*peers, ","))

	if !etcdc.SyncCluster() {
		log.Fatalf("Unable to connect to etcd cluster: %s", *peers)
	}

	switch *loglvl {
	case "debug":
		mlvl = metafora.LogLevelDebug
	case "warn":
		mlvl = metafora.LogLevelWarn
	case "error":
		mlvl = metafora.LogLevelError
	}

	hfunc := makeHandlerFunc(etcdc)
	coord := m_etcd.NewEtcdCoordinator(*name, *namespace, etcdc).(*m_etcd.EtcdCoordinator)
	bal := &metafora.DumbBalancer{}
	c := metafora.NewConsumer(coord, hfunc, bal)
	log.Printf(
		"Starting koalsmosd with etcd=%s; namespace=%s; name=%s; loglvl=%s",
		*peers, *namespace, coord.NodeID, mlvl)
	go c.Run()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, os.Kill)
	s := <-sigC
	log.Printf("Received signal %s, shutting down", s)
	c.Shutdown()
	log.Printf("Shutdown")
}
