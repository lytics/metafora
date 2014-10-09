package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"code.google.com/p/go-uuid/uuid"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora/m_etcd"
)

func main() {
	peers := flag.String("etcd", "127.0.0.1:5001", "comma delimited etcd peer list")
	namespace := flag.String("namespace", "koalemos", "metafora namespace")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Println("usage: koalemosctl [args]")
		os.Exit(1)
	}

	ec := etcd.NewClient(strings.Split(*peers, ","))

	if !ec.SyncCluster() {
		fmt.Printf("Unable to connect to etcd cluster: %s\n", *peers)
		os.Exit(2)
	}

	taskID := uuid.NewUUID().String()

	// First create the task body
	body, err := json.Marshal(&struct{ Args []string }{Args: args})
	if err != nil {
		fmt.Printf("Error marshaling task body: %v", err)
		os.Exit(3)
	}
	if _, err := ec.Set("/koalemos-tasks/"+taskID, string(body), 30*24*60*60); err != nil {
		fmt.Printf("Error creating task body: %v", err)
		os.Exit(4)
	}

	// Finally create the task for metafora
	mc := m_etcd.NewClient(*namespace, ec)
	if err := mc.SubmitTask(taskID); err != nil {
		fmt.Println("Error submitting task:", taskID)
		os.Exit(5)
	}
	fmt.Println(taskID)
}
