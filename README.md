metafora
========

[![Build Status](https://travis-ci.org/lytics/metafora.svg)](https://travis-ci.org/lytics/metafora)

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

* Balancer - Go interface consulted by *Consumer* for determining which tasks
  can be claimed and which should be released. See [balancer.go](balancer.go).
* Broker - external task and command store like
  [etcd](https://github.com/coreos/etcd).
* Consumer - core work runner. Integrates *Balancer*, *Coordinator*, and
  *Handlers* to get work done.
* Coordinator - client Go interface to *Broker*. See
  [coordinator.go](coordinator.go).
* Handler - Go interface for executing tasks.
* Task - unit of work. Executed by *Handlers*.
