package statemachine

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/lytics/metafora"
)

var ExceededErrorRate = errors.New("exceeded error rate")

type StateCode int

const (
	Runnable  StateCode = iota // Scheduled
	Sleeping                   // Scheduled, not running until time has elapsed
	Completed                  // Terminal, not scheduled
	Killed                     // Terminal, not scheduled
	Failed                     // Terminal, not scheduled
	Fault                      // Scheduled, in error handling / retry logic
	Paused                     // Scheduled, not running
)

func (s StateCode) Terminal() bool {
	switch s {
	case Runnable, Sleeping, Paused, Fault:
		return false
	case Completed, Killed, Failed:
		return true
	default:
		panic("unknown state " + strconv.Itoa(int(s)))
	}
}

func (s StateCode) String() string {
	switch s {
	case Runnable:
		return "runnable"
	case Sleeping:
		return "sleeping"
	case Completed:
		return "completed"
	case Killed:
		return "killed"
	case Failed:
		return "failed"
	case Fault:
		return "fault"
	case Paused:
		return "paused"
	default:
		panic("unknown state " + strconv.Itoa(int(s)))
	}
}

type Err struct {
	Time time.Time
	Err  string
}

// ErrHandler functions should return Run, Sleep, or Fail messages depending on
// the rate of errors.
//
// Either ErrHandler and/or StateStore should trim the error slice to keep it
// from growing without bound.
type ErrHandler func(taskID string, errs []Err) (Message, []Err)

const (
	DefaultErrLifetime = -4 * time.Hour
	DefaultErrMax      = 8
)

// DefaultErrHandler returns a Fail message if 8 errors have occurred in 4
// hours. Otherwise it enters the Sleep state for 10 minutes before trying
// again.
func DefaultErrHandler(_ string, errs []Err) (Message, []Err) {
	recent := time.Now().Add(DefaultErrLifetime)
	strikes := 0
	for _, err := range errs {
		if err.Time.After(recent) {
			strikes++
		}
	}

	if strikes >= DefaultErrMax {
		// Return a new error to transition to Failed as well as the original
		// errors to store what caused this failure.
		return Message{Code: Error, Err: ExceededErrorRate}, errs
	}
	keeperrs := errs
	if len(keeperrs) > DefaultErrMax {
		keeperrs = keeperrs[len(keeperrs)-DefaultErrMax:]
	}
	return Message{Code: Sleep, Until: time.Now().Add(10 * time.Minute)}, keeperrs
}

type State struct {
	Code   StateCode
	Until  time.Time
	Errors []Err
	//TODO Error related state?! Hm...
	//Output interface{} //XXX A way to store progress?
}

func (s State) String() string {
	switch s.Code {
	case Sleeping:
		return fmt.Sprintf("%s until %s", s.Code, s.Until)
	default:
		return s.Code.String()
	}
}

type Message struct {
	Code  MessageCode
	Until time.Time // indicates time in Sleeping state by Sleep message
	Err   error     // error associated with errMsgs
	//Output interface{} //XXX A way to store progress?
}

type MessageCode int

const (
	Run MessageCode = iota
	Sleep
	Pause
	Resume
	Kill
	Error
	Complete
	Checkpoint //XXX or NOOP?

	// Special event which triggers state machine to exit without transitioning
	// between states.
	Release
)

func (m MessageCode) String() string {
	switch m {
	case Run:
		return "run"
	case Sleep:
		return "sleep"
	case Pause:
		return "pause"
	case Resume:
		return "resume"
	case Release:
		return "release"
	case Kill:
		return "kill"
	case Error:
		return "error"
	case Complete:
		return "complete"
	case Checkpoint:
		return "checkpoint"
	default:
		panic("unknown mesage code: " + strconv.Itoa(int(m)))
	}
}

type Transition struct {
	Event MessageCode
	From  StateCode
	To    StateCode
}

func (t Transition) String() string {
	return fmt.Sprintf("%v---%v--->%v", t.From, t.Event, t.To)
}

var (
	// State Transition Table
	Rules = []Transition{
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

		// Completed, Failed, and Killed are terminal states that cannot transition
		// to anything.
	}
)

// Run a stateful handler until completion or a command is received. Handlers
// can decide whether to return the command message directly or override it
// with their own message.
type StatefulHandler func(taskID string, commands <-chan Message) Message

type StateStore interface {
	// Load the persisted or initial state for a task. Errors will cause tasks to
	// be marked as done.
	//
	// The one exception is the special error StateNotFound which will cause the
	// state machine to start from the initial (Runnable) state.
	Load(taskID string) (*State, error)

	// Store the current task state. Errors will prevent current state from being
	// persisted and prevent state transitions.
	Store(taskID string, s *State) error
}

type stateMachine struct {
	h          StatefulHandler
	ss         StateStore
	cmd        chan Message
	errHandler ErrHandler
}

// New handler that creates a state machine and exposes state transitions to
// the given handler by calling its Transition method.
//
// If ErrHandler==nil the default error handler will be used.
func New(h StatefulHandler, ss StateStore, commands chan Message, e ErrHandler) *stateMachine {
	if e == nil {
		e = DefaultErrHandler
	}
	return &stateMachine{h: h, ss: ss, cmd: commands, errHandler: e}
}

// Run the state machine enabled handler. Loads the initial state and passes
// control to the internal stateful handler.
func (s *stateMachine) Run(taskID string) (done bool) {
	state, err := s.ss.Load(taskID)
	if err != nil {
		// A failure to load the state for a task is *fatal* - the task will be
		// unscheduled and requires operator intervention to reschedule.
		metafora.Errorf("task=%q could not load initial state. Marking done! Error: %v", taskID, err)
		return true
	}
	if state.Code.Terminal() {
		metafora.Warnf("task=%q in terminal state %s - exiting.", taskID, state.Code)
		return true
	}

	// Main Run loop
	done = false
	for {
		var msg Message
		var newstate *State
		metafora.Debugf("task=%q in state %s", taskID, state.Code)

		// Execute state
		switch state.Code {
		case Runnable:
			msg = run(s.h, taskID, s.cmd)
		case Paused:
			msg = <-s.cmd
		case Sleeping:
			dur := state.Until.Sub(time.Now())
			metafora.Infof("task=%q sleeping for %s", taskID, dur)
			timer := time.NewTimer(dur)
			select {
			case <-timer.C:
				msg = Message{Code: Run}
			case msg = <-s.cmd:
				timer.Stop()
			}
		case Fault:
			// Special case where we potentially trim the current state to keep
			// errors from growing without bound.
			msg, state.Errors = s.errHandler(taskID, state.Errors)
		case Completed, Failed, Killed:
			metafora.Infof("task=%q reached terminal state %s - exiting.", taskID, state.Code)
		default:
			panic("invalid state: " + state.Code.String())
		}

		// Apply message
		newstate, ok := apply(state, msg)
		if !ok {
			metafora.Warnf("task=%q Invalid state transition=%q returned by task. Old state=%q", taskID, msg.Code, state.Code)
			msg = Message{Code: Error, Err: err}
			if newstate, ok = apply(state, msg); !ok {
				metafora.Errorf("task=%q Unable to transition to error state! Exiting with state=%q", taskID, state.Code)
				return state.Code.Terminal()
			}
		}

		// Save state
		if err := s.ss.Store(taskID, newstate); err != nil {
			metafora.Errorf("task=%q Unable to persist state=%q. Unscheduling.", taskID, newstate.Code)
			//FIXME Is this really the best thing to do?
			return true
		}

		// Set next state and loop
		state = newstate

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

func run(f StatefulHandler, tid string, cmd <-chan Message) (m Message) {
	defer func() {
		if r := recover(); r != nil {
			metafora.Errorf("task=%q Run method panic()d! Applying Error message. Panic: %v", tid, r)
			m = Message{Code: Error, Err: fmt.Errorf("%v", r)} //FIXME This is a weird way of storing the panic
		}
	}()
	m = f(tid, cmd)
	return m
}

// Stop sends a Release message to the state machine through the command chan.
func (s *stateMachine) Stop() {
	s.cmd <- Message{Code: Release}
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
