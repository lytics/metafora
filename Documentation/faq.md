Frequently Asked Questions
==========================

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
initial design.

**Q. Why not use [goworker](http://www.goworker.org/) (or similar)?**

goworker does not support rebalancing and appears to be more focused on a high
rate (>1/s) of short lived work items. Metafora is designed for a low rate
(<1/s) of long lived work items. This means rebalancing running work is
critical.

There are a lot of projects in the short-lived offline task processing space,
but few if any handle task state, rebalancing, consistent operation during
partitions, and other features critical for long running tasks.

**Q. Why not use a cluster management framework like
[Mesos](http://mesos.apache.org/) or [Kubernetes](http://kubernetes.io/)?**

You can use a cluster management framework to run Metafora, but you *shouldn't*
use Metafora as a cluster management framework.

While Metafora tasks are long lived, they're often not individually large or
necessarily resource intensive. For example, tasks in the Sleeping state stay
resident in memory to handle any wakeup events (either from a timer or external
command). Cluster management frameworks' smallest unit of work tends to be an
operating system process.

Lytics often runs over 500 tasks per server in a Metafora cluster. 500 OS
processes would incur nontrivial overhead compared to 500 Metafora tasks, not
to mention be much harder to manage.

The second reason for preferring Metafora tasks to OS processes is a much
richer command structure. Signals are the only command mechanism OS processes
have builtin. Metafora's [state machine](../statemachine/README.md) provides a
much easier to use and more featureful interface for tasks.

Cluster management frameworks are quite large in terms of code and operational
complexity -- for good reason! They're a much more powerful and general purpose
tool than Metafora. Metafora is being written, deployed, and maintained by a
very small team, so minimizing operational complexity and overhead is a key
feature.

**Q. What are Metafora's limits?**

While Lytics has not run into any firm limits, our current estimates are that
Metafora with the etcd coordinator can scale to:

* Tens of thousands of concurrently running tasks (number of servers depends on
  resource utilization of each task).
* Hundreds of state transitions (task created, sleeping, etc.) per second.

Since etcd is designed for consistency before raw throughput, it is the
limiting factor for cluster growth.

If you need more concurrent tasks or transtions it's recommended you run
multiple etcd clusters and multiple Metafora consumers. A single OS process can
run multiple Metafora consumers, so you only have to manage a single logical
Metafora cluster of servers despite there being multiple etcd clusters and
namespaces.

**Q. What does metafora mean?**

It's Greek for "transfer" and also refers to a winch on boats.
[We](http://lytics.io) borrowed the Greek naval naming theme from
[Kubernetes](http://kubernetes.io/).
