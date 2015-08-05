package statemachine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lytics/metafora"
)

var (
	MissingUntilError  = errors.New("sleeping state missing deadline")
	MissingErrorsError = errors.New("fault state has no errors")
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
		metafora.Error("unknown state: ", s)
		return false
	}
}

func (s StateCode) String() string { return string(s) }

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

func (s *State) String() string {
	switch s.Code {
	case Sleeping:
		return fmt.Sprintf("%s until %s", s.Code, s.Until)
	case Fault:
		return fmt.Sprintf("%s (%d errors)", s.Code, len(s.Errors))
	default:
		return string(s.Code)
	}
}

func (s *State) Valid() error {
	switch s.Code {
	case Completed, Failed, Killed, Paused, Runnable:
	case Sleeping:
		if s.Until == nil {
			return MissingUntilError
		}
	case Fault:
		if len(s.Errors) == 0 {
			return MissingErrorsError
		}
	default:
		return fmt.Errorf("unknown state: %q", s.Code)
	}
	return nil
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

// ErrorMessage is a simpler helper for creating error messages from an error.
func ErrorMessage(err error) *Message {
	return &Message{Code: Error, Err: err}
}

// SleepMessage is a simpler helper for creating sleep messages from a time.
func SleepMessage(t time.Time) *Message {
	return &Message{Code: Sleep, Until: &t}
}

func RunMessage() *Message        { return &Message{Code: Run} }
func PauseMessage() *Message      { return &Message{Code: Pause} }
func KillMessage() *Message       { return &Message{Code: Kill} }
func CheckpointMessage() *Message { return &Message{Code: Checkpoint} }
func ReleaseMessage() *Message    { return &Message{Code: Release} }

// Valid returns true if the Message is valid. Invalid messages sent as
// commands are discarded by the state machine.
func (m *Message) Valid() bool {
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

func (m *Message) String() string {
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

func (m MessageCode) String() string { return string(m) }

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
		{Event: Release, From: Sleeping, To: Sleeping},
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
type StatefulHandler func(task metafora.Task, commands <-chan *Message) *Message

type stateMachine struct {
	task       metafora.Task
	h          StatefulHandler
	ss         StateStore
	cl         CommandListener
	cmds       chan *Message
	errHandler ErrHandler

	mu    *sync.RWMutex
	state *State
	ts    time.Time

	stopL   *sync.Mutex
	stopped chan bool
}

// New handler that creates a state machine and exposes state transitions to
// the given handler by calling its Transition method. It should be created in
// the HandlerFunc you use with metafora's Consumer.
//
// If ErrHandler is nil DefaultErrHandler will be used.
func New(task metafora.Task, h StatefulHandler, ss StateStore, cl CommandListener, e ErrHandler) metafora.Handler {
	if e == nil {
		e = DefaultErrHandler
	}
	return &stateMachine{
		task:       task,
		h:          h,
		ss:         ss,
		cl:         cl,
		errHandler: e,
		mu:         &sync.RWMutex{},
		ts:         time.Now(),
		stopL:      &sync.Mutex{},
		stopped:    make(chan bool),
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
	s.cmds = make(chan *Message)
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
				case <-s.stopped:
					return
				}
			case <-s.stopped:
				return
			}
		}
	}()

	// Stop the command listener and internal message multiplexer when Run exits
	defer func() {
		s.cl.Stop()
		s.stop()
	}()

	tid := s.task.ID()

	// Load the initial state
	state, err := s.ss.Load(s.task)
	if err != nil {
		// A failure to load the state for a task is *fatal* - the task will be
		// unscheduled and requires operator intervention to reschedule.
		metafora.Errorf("task=%q could not load initial state. Marking done! Error: %v", tid, err)
		return true
	}
	if state == nil {
		// Note to StateStore implementors: This should not happen! Either state or
		// err must be non-nil. This code is simply to prevent a nil pointer panic.
		metafora.Errorf("statestore %T returned nil state and err for task=%q - unscheduling")
		return true
	}
	if state.Code.Terminal() {
		metafora.Warnf("task=%q in terminal state %s - exiting.", tid, state.Code)
		return true
	}

	s.setState(state) // for introspection/debugging

	// Main Statemachine Loop
	done = false
	for {
		// Enter State
		metafora.Debugf("task=%q in state %s", tid, state.Code)
		msg := s.exec(state)

		// Apply Message
		newstate, ok := apply(state, msg)
		if !ok {
			metafora.Warnf("task=%q Invalid state transition=%q returned by task. Old state=%q", tid, msg.Code, state.Code)
			msg = ErrorMessage(err)
			if newstate, ok = apply(state, msg); !ok {
				metafora.Errorf("task=%q Unable to transition to error state! Exiting with state=%q", tid, state.Code)
				return state.Code.Terminal()
			}
		}

		metafora.Infof("task=%q transitioning %s --> %s --> %s", tid, state, msg, newstate)

		// Save state
		if err := s.ss.Store(s.task, newstate); err != nil {
			metafora.Errorf("task=%q Unable to persist state=%q. Unscheduling.", tid, newstate.Code)
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

		// Alternatively Stop() may have been called but the handler may not have
		// returned the Release message. Always exit if we've been told to Stop()
		// even if the handler has returned a different Message.
		select {
		case <-s.stopped:
			return false
		default:
		}
	}
}

// execute non-terminal states
func (s *stateMachine) exec(state *State) *Message {
	switch state.Code {
	case Runnable:
		// Runnable passes control to the stateful handler
		return run(s.h, s.task, s.cmds)
	case Paused:
		// Paused until a message arrives
		return <-s.cmds
	case Sleeping:
		// Sleeping until the specified time (or a message)
		if state.Until == nil {
			metafora.Warnf("task=%q told to sleep without a time. Resuming.", s.task.ID())
			return RunMessage()
		}
		dur := state.Until.Sub(time.Now())
		metafora.Infof("task=%q sleeping for %s", s.task.ID(), dur)
		timer := time.NewTimer(dur)
		select {
		case <-timer.C:
			return RunMessage()
		case msg := <-s.cmds:
			timer.Stop()
			// Checkpoint & Release are special cases that shouldn't affect sleep
			// time, so maintain it across the state transition
			if msg.Code == Checkpoint || msg.Code == Release {
				msg.Until = state.Until
			}
			return msg
		}
	case Fault:
		// Special case where we potentially trim the current state to keep
		// errors from growing without bound.
		var msg *Message
		msg, state.Errors = s.errHandler(s.task, state.Errors)
		return msg
	default:
		panic("invalid state: " + state.String())
	}
}

func run(f StatefulHandler, task metafora.Task, cmd <-chan *Message) (m *Message) {
	defer func() {
		if r := recover(); r != nil {
			metafora.Errorf("task=%q Run method panic()d! Applying Error message. Panic: %v", task.ID(), r)
			m = &Message{Code: Error, Err: fmt.Errorf("panic: %v", r)}
		}
	}()

	// Defensive code to give handlers a *copy* of the command chan. That way if
	// a handler keeps receiving on the command chan in a goroutine past the
	// handler's lifetime it doesn't intercept commands intended for the
	// statemachine.
	internalcmd := make(chan *Message)
	stopped := make(chan struct{})
	go func() {
		for {
			select {
			case c := <-cmd:
				internalcmd <- c
			case <-stopped:
				return
			}
		}
	}()
	defer close(stopped)

	return f(task, internalcmd)
}

// Stop sends a Release message to the state machine through the command chan.
func (s *stateMachine) Stop() {
	select {
	case s.cmds <- ReleaseMessage():
		// Also inform the state machine it should exit since the internal handler
		// may override the release message causing the task to be unreleaseable.
		s.stop()
	case <-s.stopped:
		// Already stopped!
	}
}

func (s *stateMachine) stop() {
	s.stopL.Lock()
	defer s.stopL.Unlock()
	select {
	case <-s.stopped:
		return
	default:
		close(s.stopped)
	}
}

// apply a message to cause a state transition. Returns false if the state
// transition is invalid.
func apply(cur *State, m *Message) (*State, bool) {
	//XXX Is a linear scan of all rules really the best option here?
	for _, trans := range Rules {
		if trans.Event == m.Code && trans.From == cur.Code {
			metafora.Debugf("Transitioned %s", trans)
			if m.Err != nil {
				// Append errors from message
				cur.Errors = append(cur.Errors, Err{Time: time.Now(), Err: m.Err.Error()})
			}

			// New State + Message's Until + Combined Errors
			return &State{Code: trans.To, Until: m.Until, Errors: cur.Errors}, true
		}
	}
	return cur, false
}
