metafora
========

[![Build Status](https://travis-ci.org/lytics/metafora.svg?branch=master)](https://travis-ci.org/lytics/metafora) [![GoDoc](https://godoc.org/github.com/lytics/metafora?status.svg)](https://godoc.org/github.com/lytics/metafora)

Ordasity inspired distributed task runner.

Metafora is a [Go](https://golang.org) library designed to run long-running
(minutes to permanent) tasks in a cluster.

Features
--------

* **Distributed** (horizontally scalable, elastic)
* **Masterless** (work stealing, not assigning)
* **Fault tolerant** (work is reassigned if nodes disappear)
* **Simple** (few states, no checkpointing, no configuration management)
* **Extensible** (well defined interfaces for implementing balancing and
  coordinating)

Many aspects of task running are left up to the *Handler* implementation such
as checkpointing work progress, configuration management, and more complex
state transitions than Metafora provides (such as Paused, Sleep, etc.).

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
<a href="https://github.com/coreos/etcd">etcd</a> for the <i>Coordinator</i> to
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

No. Check back soon.

**Q. Where is the metaforad daemon?**

It doesn't exist. Metafora is library for you to import and use in a service
you write. Metafora handles task management but leaves implementation details
such as task implementation and daemonization up to the user.

**Q. Why not use [Ordasity](https://github.com/boundary/ordasity)?**

[We](http://lytics.io) have an existing work running system written in Go and
needed a new distribution library for it. There's over 25k lines of Go we'd
like to reuse and couldn't with Ordasity as it runs on the JVM.

**Q. Why not use [donut](https://github.com/dforsyth/donut)?**

[We](http://lytics.io) evaluated donut and found it far from production use.
While we've been inspired by many of its basic interfaces there really wasn't
much code we were interested in reusing. At ~600 lines of code in donut,
starting from scratch didn't seem like it would lose us much.

That being said we're very appreciative of donut! It heavily influenced our
design.

**Q. Why not use [goworker](http://www.goworker.org/)?**

goworker does not support rebalancing and appears to be more focused on a high
rate (>1/s) of short lived work items. Metafora is designed for a low rate
(<1/s) of long lived work items. This means rebalancing running work is
critical.

**Q. Why not use a cluster management framework like
[Mesos](http://mesos.apache.org/) or [Kubernetes](http://kubernetes.io/)?**

You can use a cluster management framework to run Metafora, but you *can't* use
Metafora as a cluster management framework.

While our tasks are long lived, they're often not large or resource intensive.
Cluster management frameworks' smallest unit of work tends to be an operating
system process. We wanted to run many tasks per process.

Cluster management frameworks are quite large in terms of code and operational
complexity -- for good reason! They're a much more powerful and general purpose
tool than Metafora. Metafora is being written, deployed, and maintained by a
very small team, so minimizing operational complexity and overhead is a key
feature.

**Q. What does metafora mean?**

It's Greek for "transfer" and also refers to a winch on boats.
[We](http://lytics.io) borrowed the Greek naval naming theme from
[Kubernetes](http://kubernetes.io/).
