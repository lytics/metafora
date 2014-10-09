## etcd

Metafora contains an [etcd](https://github.com/coreos/etcd) implementation of
the core
[`Coordinator`](https://godoc.org/github.com/lytics/metafora#Coordinator) and
[`Client`](http://godoc.org/github.com/lytics/metafora#Client) interfaces, so
that implementing Metafora with etcd in your own work system is quick and easy.

## Layout

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

### Tasks

Metafora clients submit tasks by making an empty directory in
`/<namespace>/tasks/` without a TTL.

Metafora nodes claim tasks by watching the `tasks` directory and -- if
`Balancer.CanClaim` returns `true` -- tries to create the
`/<namespace>/tasks/<tasks_id>/owner` file with the contents set to the nodes
name and a short TTL. The node must touch the file before the TTL expires
otherwise another node will claim the task and begin working on it.

Note that Metafora does not handle task parameters or configuration.

### Commands

Metafora clients send commands by making a file inside
`/<namespace>/nodes/<node_id>/commands/` with any name (preferably using a time-ordered
UUID).

Metafora nodes watch their own node's `commands` directory for new files. The
contents of the files are a command to be executed. Only one command will be
executed at a time, and pending commands are lost on node shutdown.
