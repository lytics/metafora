## Introduction

Metafora is a framework for creating highly available and distributed services written in Go.  Metafora is embeded meaning your code controls how and when metafora is started.  It uses etcd to coordinate across the nodes in your cluster.  Metafora is a leaderless task distribution system where the nodes coordinate with each other to ensure that work is evenly distributed over the cluster.

Metafora is an embedded work stealing framework built on top of etcd.

![logical1](/Documentation/images/metafora_logical_integration_diagram.png)

## Overview

Metafora gives you the ablity to build an elastic distributed application.  It makes it easy to build applications that scale in or out, and that can recover from node failures.  The following diagrams are examples of how this works.

#### Node failure or scaling in

When a node fails (or you scale in your nodes), metafora will release the tasks from the missing node back into the task pool.  Other metafora nodes will detect the unclaimed tasks and attempt to claim them.  It's important to note that metafora simply manages the reassigment of tasks; it's up to your code (possibly in your metafora handler) to clean up any bad state caused by a task crashing during processing.

![logical1](/Documentation/images/metafora_nodefailure.png)


#### Node recovery or scaling out

When a new node joins the cluster it begins picking up new tasks immediately.  Initially the other nodes may have more tasks because they've been in the cluster longer.  To address this, occasionally the members compare task load and rebalance the tasks between them.

![logical1](/Documentation/images/metafora_node_recovery.png)
