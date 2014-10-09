## Introduction 

Metafora is a framework for creating highly available and distributed services written in Go.  Metafora is embeded has to be started in your code.  It uses etcd to coordinate with other nodes in the cluster.  Its a masterless system, but the nodes coordinate with eachother to insure that work is evenly distributed between nodes. 

Metafora is an embeded work stealing framework built on top of etcd.  

![logical1](/Documentation/images/metafora_logical_integration_diagram.png) 

## Overview

Metafora gives you the ablity to build an elastic distributed application.  It makes it easy to build applications that scale in or out, and that can recover from node failures.  The following diagrams are examples of how this works. 

#### Node failure or scaling in

When a node fails (or you scale in your nodes), metafora will will release the tasks from the missing node back into the task pool.  Other metafora nodes will detect the unclaimed tasks and attempt to claim them.  It's important to note that metafora simple manages the reassigment of tasks, its upto your code (possiable in your metafora handler) to cleanup any bad state caused by a tasks crashing during processing.

![logical1](/Documentation/images/metafora_nodefailure.png)
 

#### Node recovery or scaling out

When a new node joins the cluster it begins picking up new tasks immediately.  But initially, the other nodes may have more tasks because they've been in the cluster longer.  To address this, occasionally the members compare task load and rebalance the tasks between them.  

![logical1](/Documentation/images/metafora_node_recovery.png)
