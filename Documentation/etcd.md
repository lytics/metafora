# etcd integration

Requires etcd v2 (etcd 0.4.6 works but is unsupported because it's abandoned
upstream).

Metafora contains an [etcd](https://github.com/coreos/etcd) implementation of
the core
[`Coordinator`](https://godoc.org/github.com/lytics/metafora#Coordinator) and
[`Client`](http://godoc.org/github.com/lytics/metafora#Client) interfaces, so
that implementing Metafora with etcd in your own work system is quick and easy.

## etcd layout

```
/
└── <namespace>
    ├── nodes
    │   └── <node_id>          Ephemeral
    │       └── commands  
    │           └── <command>  JSON value
    │
    ├── tasks
    │   └── <task_id>
    │       └── owner          Ephemeral, JSON value
    │
    ├── state                  Optional, only if using state store
    │   └── <task_id>          Permanent, JSON value
    │
    └── commands               Optional, only if using command listener
        └── <task_id>          Ephermeral, JSON value

```

### Tasks

Metafora clients submit tasks by making an empty directory in
`/<namespace>/tasks/` without a TTL.

Metafora nodes claim tasks by watching the `tasks` directory and -- if
`Balancer.CanClaim` returns `true` -- tries to create the
`/<namespace>/tasks/<tasks_id>/owner` file with the contents set to the nodes
name and a short TTL. The node must touch the file before the TTL expires
otherwise another node will claim the task and begin working on it.

The JSON format is:

```json
{"node": "<node ID>"}
```

Note that Metafora does not handle task parameters or configuration.

### Node Commands

Metafora clients can send node commands by making a file inside
`/<namespace>/nodes/<node_id>/commands/` with any name (preferably using a time-ordered
UUID).

Metafora nodes watch their own node's `commands` directory for new files. The
contents of the files are a command to be executed. Only one command will be
executed at a time, and pending commands are lost on node shutdown.

```json
{"command": "<command name>", "parameters": {}}
```

Where parameters is an arbitrary JSON Object.

### Task State

If you're using the etcd state store, it will persist a task's state as JSON in
`/<namespace>/state/<task_id>`.  The format of the JSON is defined by the
`statemachine` package.

Task state keys are permanent so they exist even after a task reaches a
terminal state and is unscheduled for 2 reasons:

1. Provide a task history for users to inspect or prune at their discretion.
2. Allow state store to default non-existant task states to Runnable since if
   they were running already or had run to completion before, the task key
   would exist.

See [`statemachine`'s Documentation](../statemachine/README.md) for details.

### Task Commands

If you're using the etcd commander and command listener, task commands are sent
as JSON in `/<namespace>/commands/<task_id>`. Commands are deleted after
they're handled. If more than one command is sent before either can be
processed only the last command sent will be processed.

Commands have a TTL of 1 week so they're eventually cleaned up if a task
terminates before it handles a command.

See [`statemachine`'s Documentation](../statemachine/README.md) for details.

## Useful links for managing etcd

[The etcd API](https://coreos.com/docs/distributed-configuration/etcd-api/)

[etcd cli tool](https://github.com/coreos/etcdctl)

