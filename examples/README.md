Examples
--------

Koalemos
========

Example daemon that uses Metafora for distributed task running.

Koalemos uses [etcd](https://github.com/etcd-io/etcd) to distribute tasks that
execute shell commands. It's meant as an example and **not intended for
production use.**

If you want distributed process management, please check out the following projects:

* [CoreOS/Fleet](https://coreos.com/using-coreos/clustering/)
* [Kubernetes](http://kubernetes.io/)
* [Mesos](http://mesos.apache.org/)


Installation
------------

First install `etcd` either via https://github.com/etcd-io/etcd#getting-started or the
[docker_run_etcd.sh](../scripts/docker_run_etcd.sh) script.

Then the koalemos daemon and command line tool:
```sh
go get github.com/lytics/metafora/examples/{koalemosctl,koalemosd}
```

Start the daemon in one terminal:
```sh
koalemosd
```

Start a task in the other:
```sh
koalemosctl sleep 30
```
