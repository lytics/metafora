metafora
========

[![Join the chat at https://gitter.im/lytics/metafora](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/lytics/metafora?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/lytics/metafora.svg?branch=master)](https://travis-ci.org/lytics/metafora)
[![GoDoc](https://godoc.org/github.com/lytics/metafora?status.svg)](https://godoc.org/github.com/lytics/metafora)

Metafora is a [Go](https://golang.org) library designed to run long-running
(minutes to permanent) tasks in a cluster.

IRC: `#lytics/metafora` on [irc.gitter.im](https://irc.gitter.im)

Features
--------

* **Distributed** - horizontally scalable
* **Elastic** - online cluster resizing with automated rebalancing
* **Masterless** - work stealing, not assigning, pluggable balancing
* **Fault tolerant** - tasks are reassigned if nodes disappear
* **Simple** - few states, no checkpointing, no configuration management
* **Extensible** - well defined interfaces for implementing balancing and
  coordinating
* **Exactly-once** - designed to enforce one-and-only-one instance of each
  submitted task is running<sup>[ref](Documentation/design.md#exactly-once)</sup>

Metafora is a library for building distributed task work systems. You're
responsible for creating a `main()` entrypoint for your application, writing a
`metafora.Handler` and `HandlerFunc` to actually process tasks, and then
starting Metafora's `Consumer`.

Metafora's task state machine is implemented as a `Handler` adapter. Simply
implement your task processor as a
[`StatefulHandler`](https://godoc.org/github.com/lytics/metafora/statemachine#StatefulHandler)
function, and create a `metafora.Handler` with
[`statemachine.New`](https://godoc.org/github.com/lytics/metafora/statemachine#New).

Example
-------

[koalemosd](https://github.com/lytics/metafora/blob/master/examples/koalemosd/main.go)
is a sample consumer implementation that can be run as a daemon
(it requires etcd).
[koalemosctl](https://github.com/lytics/metafora/blob/master/examples/koalemosctl/main.go)
is a sample command line client for submitting tasks to `koalemosd`.

```sh
# Install etcd as per https://github.com/etcd-io/etcd#getting-etcd
# Run the following in one terminal:
go get -v -u github.com/lytics/metafora/examples/koalemosd
koalemosd

# Run the client in another
go get -v -u github.com/lytics/metafora/examples/koalemosctl
koalemosctl sleep 3 # where "sleep 3" is any command on your $PATH
```

Since koalemosd is a simple wrapper around OS processes, it does not use the
state machine (`statemachine.StatefulHandler`).

Terms
-----

<table>
<tr>
<th>Balancer</th><td>Go interface consulted by <i>Consumer</i> for determining
which tasks can be claimed and which should be released. See <a
href="balancer.go">balancer.go</a>.</td>
</tr>
<tr>
<th>Broker</th><td>external task and command store like
<a href="https://github.com/etcd-io/etcd">etcd</a> for the <i>Coordinator</i> to
use.</td>
</tr>
<th>Consumer</th><td>core work runner. Integrates <i>Balancer</i>,
<i>Coordinator</i>, and <i>Handlers</i> to get work done.</td>
</tr>
<tr>
<th>Coordinator</th><td>client Go interface to <i>Broker</i>. See
<a href="coordinator.go">coordinator.go</a>.</td>
</tr>
<tr>
<th>Handler</th><td>Go interface for executing tasks.</td>
</tr>
<tr>
<th>Task</th><td>unit of work. Executed by <i>Handlers</i>.</td>
</tr>
</table>

FAQ
---

**Q. Is it ready for production use?**

*Yes.* Metafora with the etcd coordinator has been the production work system at
[Lytics](http://lytics.io) since January 2014 and runs thousands of tasks
concurrently across a cluster of VMs.

Since Metafora is still under heavy development, you probably want to pin the
dependencies to a commit hash or
[tag](https://github.com/lytics/metafora/releases) to keep the API stable. The
`master` branch is automatically tested and is safe for use if you can tolerate
API changes.

**Q. Where is the metaforad daemon?**

It doesn't exist. Metafora is library for you to import and use in a service
you write. Metafora handles task management but leaves implementation details
such as task implementation and daemonization up to the user.

[FAQ continued in Documentation...](Documentation/faq.md)
