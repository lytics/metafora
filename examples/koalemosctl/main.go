package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora/examples/koalemos"
	"github.com/lytics/metafora/m_etcd"
)

func main() {
	peers := flag.String("etcd", "127.0.0.1:4001", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("usage: koalemosctl [args]")
		os.Exit(1)
	}

	hosts := strings.Split(*peers, ",")
	ec := etcd.NewClient(hosts)

	if !ec.SyncCluster() {
		fmt.Printf("Unable to connect to etcd cluster: %s\n", *peers)
		os.Exit(2)
	}

	rand.Seed(time.Now().UnixNano())

	task := koalemos.NewTask(fmt.Sprintf("%x", rand.Int63()))
	task.Args = args

	// Finally create the task for metafora
	mc := m_etcd.NewClient(*namespace, hosts)
	if err := mc.SubmitTask(task); err != nil {
		fmt.Println("Error submitting task:", task.ID())
		os.Exit(2)
	}
	fmt.Println(task.ID())
}
