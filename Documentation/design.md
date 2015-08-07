# Design

## Exactly Once

Metafora makes a *best effort* to ensure that exactly one instance of a
submitted task is executing in a cluster. In other words, for task `T`, only
one node, may be executing
[`HandlerFunc(T).Run`](https://godoc.org/github.com/lytics/metafora#Handler).

### Implementation

*Implementations are Coordinator specific, so this covers the builtin etcd
coordinator.*

Task claims are represented as keys with a TTL in etcd. A claim key is
refreshed before the TTL expires in order to ensure the node running the task
maintains the claim as long as the node is still executing normally.

If the node ceases to execute normally due to a crash, high CPU utilization,
network partition between the node and etcd, a bug, etc. the claim in etcd will
expire and the task will be available for claiming by another node. When the
problematic coordinator detects it has failed to maintain its claim, it informs
the consumer it has `Lost` the task, the consumer calls `Handler.Stop` on the
task, and ideally the task exits before it starts executing on a new node (see
Limitations below).

If a node is unable to reliably communicate with etcd it will stop all of its
tasks and release all of its claims, effectively leaving the cluster. It will
begin claiming tasks once reliable communication with etcd is restored
(although it will probably have to wait on other nodes to `Rebalance` tasks
first).

All communication with etcd is done with strong consistency.

### Limitations

Metafora cannot stop `Handler.Run` from continuing to execute the moment its
claim expires. Goroutines are cooperative and threads of execution are subject
to arbitrary pauses and scheduling.

Using the etcd coordinator, if `Handler.Run` does not exit within 30 seconds,
the task is eligible for simultaneous execution on multiple nodes.<sup>#139</sup>

In other words: the "exactly once guarantee" relies on well behaved user code
and accurate timers - both of which are out of Metafora's control.

Handler's should be designed to exit as quickly as possible when `Stop` is
called if they rely on Metafora's exactly-once behavior. Tasks which shutdown
slowly should be written to tolerate at-least-once semantics.
