package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lytics/metafora"
	"github.com/lytics/metafora/examples/koalemos"
	"github.com/lytics/metafora/metcdv3"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	mlvl := metafora.LogLevelInfo
	hostname, _ := os.Hostname()

	peers := flag.String("etcd", "http://127.0.0.1:2379", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	name := flag.String("name", hostname, "node name or empty for automatic")
	loglvl := flag.String("log", mlvl.String(), "set log level: [debug], info, warn, error")
	flag.Parse()

	hosts := strings.Split(*peers, ",")
	etcdv3c, err := etcdv3.NewFromURLs(hosts)
	if err != nil {
		fmt.Printf("Unable to create to etcd clientv3: %s\n", err)
		os.Exit(2)
	}

	if err := etcdv3c.Sync(context.Background()); err != nil {
		fmt.Printf("Unable to connect to etcd cluster: %s\n", *peers)
		os.Exit(2)
	}

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

	conf := metcdv3.NewConfig(*name, *namespace)

	// Replace NewTask func with one that returns a *koalemos.Task
	conf.NewTaskFunc = func(id, value string) metafora.Task {
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

	hfunc := makeHandlerFunc(etcdv3c)
	ec := metcdv3.NewEtcdV3Coordinator(conf, etcdv3c)
	if err != nil {
		metafora.Errorf("Error creating etcd coordinator: %v", err)
	}

	filter := func(_ *metcdv3.FilterableValue) bool { return true }
	bal := metcdv3.NewFairBalancer(conf, etcdv3c, filter)
	c, err := metafora.NewConsumer(ec, hfunc, bal)
	if err != nil {
		metafora.Errorf("Error creating consumer: %v", err)
		os.Exit(2)
	}
	metafora.Infof(
		"Starting koalsmosd with etcd=%s; namespace=%s; name=%s; loglvl=%s",
		*peers, conf.Namespace, conf.Name, mlvl)
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
