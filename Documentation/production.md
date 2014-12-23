Running Metafora in Production
==============================

Metafora is **not production ready.** At least not for the faint of heart.

[Lytics](https://lytics.github.io/) *is using Metafora in production.* There's
a lot that goes into deciding whether a project is "production ready" and what
one project considers a 1.0 release isn't really much of an indicator it fits
everyone's definitions of production ready.

We'll try to keep this document up to date to help people decide if Metafora is
ready for their use.

Core Consumer
-------------

**Almost ready**

* [x] Task Claiming
* [x] panic()s from task handlers are recovered
* [x] Rebalancing called periodically
* [x] Graceful shutdown (shutdown blocks until all tasks are released)
* [x] Resilient to transient network issues *(extremely simplistic logic)*
* [ ] Deadlocked task handlers block shutdown
* [ ] Deadlocked task handlers block rebalacing 
* [ ] Smart balancing (Dumb balancing works!)
* Commands receiving
  * [x] Freeze/Unfreeze
  * [ ] Everything else is untested

API Stability
-------------

**Not ready**

Expect the API to change.

However, the core flow of creating a `Consumer` with a `Coordinator` and
`Handler` -- as well as the `Coordinator` and `Handler` interfaces -- are
unlikely to change substantially.

Feature Completeness
--------------------

**Ready**

Advertised features are complete.

More balancers would be nice, but most users will probably want to implement
their own.

Alternative coordinators would be nice, but Lytics doesn't have plans to use
anything but etcd in production.

Internally Lytics has complex task configuration, state tracking, and command
delivery. We're seeking to open source them in the future.

etcd Coordinator
----------------

**Ready**

Lytics is successfully using the etcd coordinator in production with etcd 0.4.6
and has tested against etcd 2.0 pre-releases.

koalemos example
----------------

**Never ready**

Koalemos is intended to be an example Metafora application and client and will
probably never be ready for production use. Anyone interested in using Metafora
to run arbitrary subprocesses are encouraged to create a new project. We'd be
happy to point to it as an example!
