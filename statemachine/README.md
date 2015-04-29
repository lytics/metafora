# Metafora Finite State Machine

The `statemachine` package provides a featureful state machine for us by
Metafora task handlers.

## Features

* Static state machine; no custom states or messages (transitions)
* Per task state machine; task may intercept commands
* Flexible state store (see `StateStore` interface)
* Flexible command sending/receiving (see `Commander`, `CommandListener`, or
  [the etcd implementation](../m_etcd/commander.go).
* Flexible error handling with builtin retry logic (see
  [`errors.go`](errors.go)).
* States: Runnable, Paused, Sleeping, Fault, Completed, Failed, Killed
* Commands/Messages: Run, Pause, Sleep, Release, Error, Kill, Complete, Checkpoint
* Tasks in a terminal state are unscheduled and will take no cluster resources.

## Control Flow

1. Coordinator receives a claimable task from a Watch
2. Consumer calls `Balancer.CanClaim(task)`
3. If claimable, Consumer calls `Coordinator.Claim(task)` to claim it.
4. If claim was successful, Consumer starts the task handler which is created
   by `statemachine.New(...)`.
5. State machine loads initial state via `StateStore.Load(task)`.
6. If the task is `Runnable` hand over control to the `StatefulHandler`
   implementation provided by the user.
7. Run until task returns a `Message` either due to completion, an error, or a
   received command.

There are quite a few moving parts that are hooked together:

* The Consumer needs a `Coordinator`, `Balancer`, and `HandlerFunc` like
  normal, but you should use `statemachine.New(...)` to create the `Handler`
  returned by your `HandlerFunc`.
* The state machine requires a `StateStore` and `CommandListener`. The `m_etcd`
  package includes an etcd implemenation of `CommandLister` (as well as
  `Commander` for sending commands), but no default `StateStore` is provided.
* Your task handling code must be implemented in a function (or method) that
  fulfills the `StatefulHandler` signature. When your handler receives a
  command it should return it (or override it with a new `Message`) to the
  state machine to handle state transitions.

## States

State | Description
------|------------
Runnable | Task is runnable and control is passed to the task handler.
Paused | Task is paused until a command is received.
Sleeping | Task is paused until a specified time (or a command is received).
Fault | An error occurred and a custom error handler is invoked.
Completed | **Terminal** Task returned the `Complete` message because it finished succesfully.
Failed | **Terminal** The error handler executed during the Fault state determined the task has failed permanently.
Killed | **Terminal** Task received a `Kill` message.

## Messages

AKA Events or Commands

Messages cause transitions between states.

Message | Description
--------|------------
Run | Causes a `Paused` or `Sleeping` task to transition to `Runnable` and begin executing.
Pause | Causes a `Runnable` or `Sleeping` task to transition to `Paused`.
Sleep | Requires an `Until time.Time` to be set. Causes non-terminal states to pause until the time is reached.
Error | Requires an `Err error` to be set. Usually returned by tasks to transition to `Fault` state.
Release | *See below*
Checkpoint | *See below*
Kill  | Causes a non-terminal state to transition to `Killed`.
Complete | Should only be returned by tasks. Causes a `Runnable` state to transition to `Completed`.


### Release

Release is a special message that does *not* transition between states. Instead
the task handler exits and the Coordinator's claim on the task is released.

Metafora's `Handler.Stop()` method sends the `Release` command to a running
task to request it exit. It's most often used when cleanly restarting Metafora
nodes.

### Checkpoint

Checkpoint is a special message that - like `Release` - does *not* transition
between states. It is meant to be a signal to tasks to persist any internal
state and optionally exit to allow the state machine to store.

Since a `Checkpoint` is a noop in the state machine a task may decide to
intercept the message and *not* return.
