## Introduction 

Metafora is a framework for creating highly available and distributed services written in Go.  Metafora is embeded has to be started in your code.  It uses etcd to coordinate with other nodes in the cluster.  Its a masterless system, but the nodes coordinate with eachother to insure that work is evenly distributed between nodes. 

Metafora is an embeded work stealing framework built on top of etcd.  

<a href="url"><img src="/Documentation/images/metafora_logical_integration_diagram.png" align="left" height="400" width="430" ></a>

## Overview

Metafora gives you the ablity to build an elastic distributed application.  It makes it easy to build applications that scale in or out, and that can recover from node failures.  The following diagrams are examples of how this works. 

#### Node failure or scaling in

When a node fails (or you scale in your nodes), metafora will will release the tasks from the missing node back into the task pool.  Other metafora nodes will detect the unclaimed tasks and attempt to claim them.  It's important to note that metafora simple manages the reassigment of tasks, its upto your code (possiable in your metafora handler) to cleanup any bad state caused by a tasks crashing during processing.

<a href="url"><img src="/Documentation/images/metafora_nodefailure.png" align="left" height="400" width="380" ></a>

#### Node recovery or scaling out

When a new node joins the cluster it begins picking up new tasks immediately.  But initially, the other nodes may have more tasks because they've been in the cluster longer.  To address this, occasionally the members compare task load and rebalance the tasks between them.  

<a href="url"><img src="/Documentation/images/metafora_node_recovery.png" align="left" height="400" width="380" ></a>




## etcd integration

Metafora contains an [etcd](https://github.com/coreos/etcd) implementation of
the core
[`Coordinator`](https://godoc.org/github.com/lytics/metafora#Coordinator) and
[`Client`](http://godoc.org/github.com/lytics/metafora#Client) interfaces, so
that implementing Metafora with etcd in your own work system is quick and easy.

##### etcd layout

```
/
└── <namespace>
    ├── nodes
    │   └── <node_id>          Ephemeral
    │       └── commands  
    │           └── <command>  contents are the command name
    └── tasks
        └── <task_id>
            └── owner          Ephemeral
                               contents are the owning node
```

##### Tasks

Metafora clients submit tasks by making an empty directory in
`/<namespace>/tasks/` without a TTL.

Metafora nodes claim tasks by watching the `tasks` directory and -- if
`Balancer.CanClaim` returns `true` -- tries to create the
`/<namespace>/tasks/<tasks_id>/owner` file with the contents set to the nodes
name and a short TTL. The node must touch the file before the TTL expires
otherwise another node will claim the task and begin working on it.

Note that Metafora does not handle task parameters or configuration.

##### Commands

Metafora clients send commands by making a file inside
`/<namespace>/nodes/<node_id>/commands/` with any name (preferably using a time-ordered
UUID).

Metafora nodes watch their own node's `commands` directory for new files. The
contents of the files are a command to be executed. Only one command will be
executed at a time, and pending commands are lost on node shutdown.


