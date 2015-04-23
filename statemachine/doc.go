// Statemachine is a featureful statemachine implementation for Metafora
// handlers to use. It is implemented as a Handler wrapper which provides a
// channel of incoming commands to wrapped handlers. Internal handlers are
// expected to shutdown cleanly and exit upon receiving a command from the
// state machine. The state machine will handle the state transition and
// restart the internal handler if necesary.
//
// Users must provide a StateStore implementation for persisting task state.
package statemachine
