Examples
--------

Koalemos
========

Example daemon that uses Metafora for distributed task running.

Koalemos uses [etcd](https://github.com/coreos/etcd) to distribute tasks that
execute shell commands. It's meant as an example and **not intended for
production use.**

If you want distributed process management, please check out the following projects:

* [CoreOS/Fleet](https://coreos.com/using-coreos/clustering/)
* [Kubernetes](http://kubernetes.io/)
* [Mesos](http://mesos.apache.org/)


Installation
------------

First install `etcd`: https://github.com/coreos/etcd#getting-started

Then the koalemos daemon and command line tool:
```sh
go get github.com/lytics/metafora/examples/{koalesmosctl,koalemosd}
```

Start the daemon in one terminal:
```sh
koalemosd
```

Start a task in the other:
```sh
koalemosctl sleep 30
```

Whatever you do, **don't do this:**
```sh
koalesmosctl koalesmosctl
```
