package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/client"
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

	etcdconfig := client.Config{Endpoints: strings.Split(*peers, ",")}
	etcdclient, err := client.New(etcdconfig)
	if err != nil {
		fmt.Printf("Error creating etcd client: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := etcdclient.Sync(ctx); err != nil {
		fmt.Printf("Unable to connect to etcd cluster %q: %v\n", *peers, err)
		os.Exit(2)
	}

	rand.Seed(time.Now().UnixNano())

	task := koalemos.NewTask(fmt.Sprintf("%x", rand.Int63()))
	task.Args = args

	// Finally create the task for metafora
	mc := m_etcd.NewClient(*namespace, etcdconfig.Endpoints)
	if err := mc.SubmitTask(task); err != nil {
		fmt.Println("Error submitting task:", task.ID())
		os.Exit(2)
	}
	fmt.Println(task.ID())
}
