package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/lytics/metafora/examples/koalemos"
	"github.com/lytics/metafora/metcdv3"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	peers := flag.String("etcd", "127.0.0.1:2379", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("usage: koalemosctl [args]")
		os.Exit(1)
	}

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

	rand.Seed(time.Now().UnixNano())

	task := koalemos.NewTask(fmt.Sprintf("%x", rand.Int63()))
	task.Args = args

	// Finally create the task for metafora
	mc := metcdv3.NewClient(*namespace, etcdv3c)
	if err := mc.SubmitTask(task); err != nil {
		fmt.Println("Error submitting task:", task.ID())
		os.Exit(2)
	}
	fmt.Println(task.ID())
}
