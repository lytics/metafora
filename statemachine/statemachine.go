package statemachine

import (
	"fmt"
	"sync"
	"time"

	"github.com/lytics/metafora"
)

// StateCode is the actual state key. The State struct adds additional metadata
// related to certain StateCodes.
type StateCode string

const (
	Runnable  StateCode = "runnable"  // Scheduled
	Sleeping  StateCode = "sleeping"  // Scheduled, not running until time has elapsed
	Completed StateCode = "completed" // Terminal, not scheduled
	Killed    StateCode = "killed"    // Terminal, not scheduled
	Failed    StateCode = "failed"    // Terminal, not scheduled
	Fault     StateCode = "fault"     // Scheduled, in error handling / retry logic
	Paused    StateCode = "paused"    // Scheduled, not running
)

// Terminal states will never run and cannot transition to a non-terminal
// state.
func (s StateCode) Terminal() bool {
	switch s {
	case Runnable, Sleeping, Paused, Fault:
		return false
	case Completed, Killed, Failed:
		return true
	default:
		panic("unknown state " + s)
	}
}

// State represents the current state of a stateful handler. See StateCode for
// details. Until and Errors are extra state used by the Sleeping and Fault
// states respectively.
type State struct {
	Code   StateCode  `json:"state"`
	Until  *time.Time `json:"until,omitempty"`
	Errors []Err      `json:"errors,omitempty"`
}

// copy state so mutations to Until and Errors aren't shared.
func (s *State) copy() *State {
	ns := &State{Code: s.Code}
	if s.Until != nil {
		until := *s.Until
		ns.Until = &until
	}
	for i := range s.Errors {
		ns.Errors = append(ns.Errors, s.Errors[i])
	}
	return ns
}

func (s State) String() string {
	switch s.Code {
	case Sleeping:
		return fmt.Sprintf("%s until %s", s.Code, s.Until)
	case Fault:
		return fmt.Sprintf("%s (%d errors)", s.Code, len(s.Errors))
	default:
		return string(s.Code)
	}
}

// Messages are events that cause state transitions. Until and Err are used by
// the Sleep and Error messages respectively.
type Message struct {
	Code MessageCode `json:"message"`

	// Until is when the statemachine should transition from sleeping to runnable
	Until *time.Time `json:"until,omitempty"`

	// Err is the error that caused this Error message
	Err error `json:"error,omitempty"`
}

// Valid returns true if the Message is valid. Invalid messages sent as
// commands are discarded by the state machine.
func (m Message) Valid() bool {
	switch m.Code {
	case Run, Pause, Release, Checkpoint, Complete, Kill:
		return true
	case Sleep:
		return m.Until != nil
	case Error:
		return m.Err != nil
	default:
		return false
	}
}

func (m Message) String() string {
	switch m.Code {
	case Sleep:
		if m.Until != nil {
			return fmt.Sprintf("%s until %s", m.Code, m.Until)
		}
	case Error:
		if m.Err != nil {
			return fmt.Sprintf("%s: %s", m.Code, m.Err.Error())
		}
	}
	return string(m.Code)
}

// MessageCode is the symbolic name of a state transition.
type MessageCode string

const (
	Run        MessageCode = "run"
	Sleep      MessageCode = "sleep"
	Pause      MessageCode = "pause"
	Kill       MessageCode = "kill"
	Error      MessageCode = "error"
	Complete   MessageCode = "complete"
	Checkpoint MessageCode = "checkpoint"

	// Special event which triggers state machine to exit without transitioning
	// between states.
	Release MessageCode = "release"
)

// Transitions represent a state machine transition from one state to another
// given an event message.
type Transition struct {
	Event MessageCode
	From  StateCode
	To    StateCode
}

func (t Transition) String() string {
	return fmt.Sprintf("%v---%v--->%v", t.From, t.Event, t.To)
}

var (
	// Rules is the state transition table.
	Rules = [...]Transition{
		// Runnable can transition to anything
		{Event: Checkpoint, From: Runnable, To: Runnable},
		{Event: Release, From: Runnable, To: Runnable},
		{Event: Sleep, From: Runnable, To: Sleeping},
		{Event: Complete, From: Runnable, To: Completed},
		{Event: Kill, From: Runnable, To: Killed},
		{Event: Error, From: Runnable, To: Fault},
		{Event: Pause, From: Runnable, To: Paused},

		// Sleeping can return to Runnable or be Killed/Paused
		{Event: Checkpoint, From: Sleeping, To: Sleeping},
		{Event: Sleep, From: Sleeping, To: Sleeping},
		{Event: Run, From: Sleeping, To: Runnable},
		{Event: Kill, From: Sleeping, To: Killed},
		{Event: Pause, From: Sleeping, To: Paused},
		{Event: Error, From: Sleeping, To: Fault},

		// The error state transitions to either sleeping, failed, or released (to
		// allow custom error handlers to workaround localitly related errors).
		{Event: Sleep, From: Fault, To: Sleeping},
		{Event: Error, From: Fault, To: Failed},

		// Paused can return to Runnable, be put to Sleep, or Killed
		{Event: Checkpoint, From: Paused, To: Paused},
		{Event: Release, From: Paused, To: Paused},
		{Event: Run, From: Paused, To: Runnable},
		{Event: Sleep, From: Paused, To: Sleeping},
		{Event: Kill, From: Paused, To: Killed},
		{Event: Pause, From: Paused, To: Paused},

		// Completed, Failed, and Killed are terminal states that cannot transition
		// to anything.
	}
)

// StatefulHandler is the function signature that the state machine is able to
// run. Instead of metafora.Handler's Stop method, StatefulHandlers receive
// Messages via the commands chan and return their exit status via a Message.
//
// Normally StatefulHandlers simply return a Message as soon as it's received
// on the commands chan. However, it's also acceptable for a handler to return
// a different Message. For example if it encounters an error during shutdown,
// it may choose to return that error as an Error Message as opposed to the
// original command.
type StatefulHandler func(taskID string, commands <-chan Message) Message

type stateMachine struct {
	taskID     string
	h          StatefulHandler
	ss         StateStore
	cl         CommandListener
	cmds       chan Message
	errHandler ErrHandler

	mu    *sync.RWMutex
	state *State
	ts    time.Time
}

// New handler that creates a state machine and exposes state transitions to
// the given handler by calling its Transition method. It should be created in
// the HandlerFunc you use with metafora's Consumer.
//
// If ErrHandler is nil DefaultErrHandler will be used.
func New(tid string, h StatefulHandler, ss StateStore, cl CommandListener, e ErrHandler) metafora.Handler {
	if e == nil {
		e = DefaultErrHandler
	}
	return &stateMachine{
		taskID:     tid,
		h:          h,
		ss:         ss,
		cl:         cl,
		errHandler: e,
		mu:         &sync.RWMutex{},
		ts:         time.Now(),
	}
}

// State returns the current state the state machine is in and what time it
// entered that state. The State may be nil if Run() has yet to be called.
func (s *stateMachine) State() (*State, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, s.ts
}

func (s *stateMachine) setState(state *State) {
	s.mu.Lock()
	s.state = state.copy()
	s.ts = time.Now()
	s.mu.Unlock()
}

// Run the state machine enabled handler. Loads the initial state and passes
// control to the internal stateful handler passing commands from the command
// listener into the handler's commands chan.
func (s *stateMachine) Run() (done bool) {
	// Multiplex external (Stop) messages and internal ones
	stopped := make(chan struct{})
	s.cmds = make(chan Message)
	go func() {
		for {
			select {
			case m := <-s.cl.Receive():
				if !m.Valid() {
					metafora.Warnf("Ignoring invalid command: %q", m)
					continue
				}
				select {
				case s.cmds <- m:
				case <-stopped:
					return
				}
			case <-stopped:
				return
			}
		}
	}()

	// Stop the command listener and internal message multiplexer when Run exits
	defer func() {
		s.cl.Stop()
		close(stopped)
	}()

	// Load the initial state
	state, err := s.ss.Load(s.taskID)
	if err != nil {
		// A failure to load the state for a task is *fatal* - the task will be
		// unscheduled and requires operator intervention to reschedule.
		metafora.Errorf("task=%q could not load initial state. Marking done! Error: %v", s.taskID, err)
		return true
	}
	if state.Code.Terminal() {
		metafora.Warnf("task=%q in terminal state %s - exiting.", s.taskID, state.Code)
		return true
	}
	s.setState(state)

	// Main Run loop
	done = false
	for {
		metafora.Debugf("task=%q in state %s", s.taskID, state.Code)
		msg := s.exec(state)

		// Enter State
		// Apply Message
		newstate, ok := apply(state, msg)
		if !ok {
			metafora.Warnf("task=%q Invalid state transition=%q returned by task. Old state=%q", s.taskID, msg.Code, state.Code)
			msg = Message{Code: Error, Err: err}
			if newstate, ok = apply(state, msg); !ok {
				metafora.Errorf("task=%q Unable to transition to error state! Exiting with state=%q", s.taskID, state.Code)
				return state.Code.Terminal()
			}
		}

		metafora.Infof("task=%q transitioning %s --> %s --> %s", s.taskID, state, msg, newstate)

		// Save state
		if err := s.ss.Store(s.taskID, newstate); err != nil {
			metafora.Errorf("task=%q Unable to persist state=%q. Unscheduling.", s.taskID, newstate.Code)
			return true
		}

		// Set next state and loop if non-terminal
		state = newstate

		// Expose the state for introspection
		s.setState(state)

		// Exit and unschedule task on terminal state.
		if state.Code.Terminal() {
			return true
		}

		// Release messages indicate the task should exit but not unschedule.
		if msg.Code == Release {
			return false
		}
	}
}

// execute non-terminal states
func (s *stateMachine) exec(state *State) Message {
	switch state.Code {
	case Runnable:
		// Runnable passes control to the stateful handler
		return run(s.h, s.taskID, s.cmds)
	case Paused:
		// Paused until a message arrives
		return <-s.cmds
	case Sleeping:
		// Sleeping until the specified time (or a message)
		if state.Until == nil {
			metafora.Warnf("task=%q told to sleep without a time. Resuming.", s.taskID)
			return Message{Code: Run}
		}
		dur := state.Until.Sub(time.Now())
		metafora.Infof("task=%q sleeping for %s", s.taskID, dur)
		timer := time.NewTimer(dur)
		select {
		case <-timer.C:
			return Message{Code: Run}
		case msg := <-s.cmds:
			timer.Stop()
			// Checkpoint is a special case that shouldn't affect sleep time, so
			// maintain it across the state transition
			if msg.Code == Checkpoint {
				msg.Until = state.Until
			}
			return msg
		}
	case Fault:
		// Special case where we potentially trim the current state to keep
		// errors from growing without bound.
		var msg Message
		msg, state.Errors = s.errHandler(s.taskID, state.Errors)
		return msg
	default:
		panic("invalid state: " + state.String())
	}
}

func run(f StatefulHandler, tid string, cmd <-chan Message) (m Message) {
	defer func() {
		if r := recover(); r != nil {
			metafora.Errorf("task=%q Run method panic()d! Applying Error message. Panic: %v", tid, r)
			m = Message{Code: Error, Err: fmt.Errorf("panic: %v", r)}
		}
	}()
	m = f(tid, cmd)
	return m
}

// Stop sends a Release message to the state machine through the command chan.
func (s *stateMachine) Stop() {
	s.cmds <- Message{Code: Release}
}

// apply a message to cause a state transition. Returns false if the state
// transition is invalid.
func apply(cur *State, m Message) (*State, bool) {
	//XXX Is a linear scan of all rules really the best option here?
	for _, trans := range Rules {
		if trans.Event == m.Code && trans.From == cur.Code {
			metafora.Debugf("Transitioned %s", trans)
			return &State{Code: trans.To, Until: m.Until}, true
		}
	}
	return cur, false
}
