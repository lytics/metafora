package main

import (
	"encoding/json"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/examples/koalemos"
	"github.com/lytics/metafora/m_etcd"
)

func main() {
	mlvl := metafora.LogLevelInfo

	peers := flag.String("etcd", "http://127.0.0.1:2379", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	name := flag.String("name", "", "node name or empty for automatic")
	loglvl := flag.String("log", mlvl.String(), "set log level: [debug], info, warn, error")
	flag.Parse()

	hosts := strings.Split(*peers, ",")
	etcdc := etcd.NewClient(hosts)

	switch strings.ToLower(*loglvl) {
	case "debug":
		mlvl = metafora.LogLevelDebug
	case "info":
		mlvl = metafora.LogLevelInfo
	case "warn":
		mlvl = metafora.LogLevelWarn
	case "error":
		mlvl = metafora.LogLevelError
	default:
		metafora.Warnf("Invalid log level %q - using %s", *loglvl, mlvl)
	}
	metafora.SetLogLevel(mlvl)

	hfunc := makeHandlerFunc(etcdc)
	ec, err := m_etcd.NewEtcdCoordinator(*name, *namespace, hosts)
	if err != nil {
		metafora.Errorf("Error creating etcd coordinator: %v", err)
	}

	// Replace NewTask func with one that returns a *koalemos.Task
	ec.NewTask = func(id, value string) metafora.Task {
		t := koalemos.NewTask(id)
		if value == "" {
			return t
		}
		if err := json.Unmarshal([]byte(value), t); err != nil {
			metafora.Errorf("Unable to unmarshal task %s: %v", t.ID(), err)
			return nil
		}
		return t
	}

	bal := m_etcd.NewFairBalancer(*name, *namespace, hosts)
	c, err := metafora.NewConsumer(ec, hfunc, bal)
	if err != nil {
		metafora.Errorf("Error creating consumer: %v", err)
		os.Exit(2)
	}
	metafora.Infof(
		"Starting koalsmosd with etcd=%s; namespace=%s; name=%s; loglvl=%s",
		*peers, *namespace, ec.NodeID, mlvl)
	consumerRunning := make(chan struct{})
	go func() {
		defer close(consumerRunning)
		c.Run()
	}()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, os.Interrupt, os.Kill, syscall.SIGTERM)
	select {
	case s := <-sigC:
		metafora.Infof("Received signal %s, shutting down", s)
	case <-consumerRunning:
		metafora.Warn("Consumer exited. Shutting down.")
	}
	c.Shutdown()
	metafora.Info("Shutdown")
}
