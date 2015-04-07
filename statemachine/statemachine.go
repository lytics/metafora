package statemachine

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/lytics/metafora"
)

type StateCode int

const (
	Runnable  StateCode = iota // Scheduled
	Sleeping                   // Scheduled, not running until time has elapsed
	Completed                  // Terminal, not scheduled
	Killed                     // Terminal, not scheduled
	Failed                     // Terminal, not scheduled
	Error                      // Scheduled, in error handling / retry logic
	Paused                     // Scheduled, not running
)

func (s StateCode) Terminal() bool {
	switch s {
	case Runnable, Sleeping, Paused, Error:
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
	case Error:
		return "error"
	case Paused:
		return "paused"
	default:
		panic("unknown state " + strconv.Itoa(int(s)))
	}
}

type State struct {
	Code  StateCode
	Until time.Time
	//TODO Error related state?! Hm...
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
}

type MessageCode int

const (
	// Public/explicit
	Run MessageCode = iota
	Sleep
	Pause
	Resume
	Release
	Kill

	// Internal/implicit
	errMsg
	doneMsg
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
	case errMsg:
		return "error"
	case doneMsg:
		return "done"
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
	Rules = []Transition{
		// Runnable can transition to anything
		{Event: Run, From: Runnable, To: Runnable},
		{Event: Release, From: Runnable, To: Runnable},
		{Event: Sleep, From: Runnable, To: Sleeping},
		{Event: doneMsg, From: Runnable, To: Completed},
		{Event: Kill, From: Runnable, To: Killed},
		{Event: errMsg, From: Runnable, To: Error},
		{Event: Pause, From: Runnable, To: Paused},

		// Sleeping can return to Runnable or be Killed/Paused
		{Event: Sleep, From: Sleeping, To: Sleeping},
		{Event: Run, From: Sleeping, To: Runnable},
		{Event: Kill, From: Sleeping, To: Killed},
		{Event: Pause, From: Sleeping, To: Paused},

		// Error can return to Runnable or be put into any other state but Completed
		{Event: Run, From: Error, To: Runnable},
		{Event: Sleep, From: Error, To: Sleeping},
		{Event: Pause, From: Error, To: Paused},
		{Event: Kill, From: Error, To: Killed},
		{Event: errMsg, From: Error, To: Failed},

		// Paused can return to Runnable, be put to Sleep, or Killed
		{Event: Run, From: Paused, To: Runnable},
		{Event: Sleep, From: Paused, To: Sleeping},
		{Event: Kill, From: Paused, To: Killed},

		// Completed, Failed, and Killed are terminal states that cannot transition
		// to anything.
	}
)

type StatefulHandler interface {
	// Run a stateful handler
	Run(sm StateMachine, taskID string) (done bool)

	// Transitioned between states. This method is called and blocks state
	// machine execution until it returns.
	//
	// StatefulHandler Run methods must exit after Transition is called with a
	// Terminal state as it's impossible to transition out of Terminal states.
	//
	// Transition must not try to Apply a new state event.
	//
	//XXX Should this be blocking? Is letting it return an error a good idea?
	Transition(t Transition) error
}

type StateStore interface {
	// Load the persisted or initial state for a task. Errors will cause tasks to
	// be marked as done.
	Load(taskID string) (State, error)

	// Store the current task state. Errors will prevent current state from being
	// persisted and prevent state transitions.
	Store(taskID string, s State) error
}

// StateMachine interface provided to StatefulHandler.
type StateMachine interface {
	Apply(m Message) (State, error)
	State() State
}

type stateMachine struct {
	// initialized is closed once handler.Run has passed control over to the
	// internal handler. It's necessary to prevent handler.Stop from trying to
	// use an uninitialized state machine.
	initialized chan bool

	h   StatefulHandler
	tid string
	ss  StateStore

	mu *sync.Mutex
	s  State
}

// New handler that creates a state machine and exposes state transitions to
// the given handler by calling its Transition method.
func New(h StatefulHandler, ss StateStore) *stateMachine {
	return &stateMachine{h: h, ss: ss, initialized: make(chan bool)}
}

// Run the state machine enabled handler. Loads the initial state and passes
// control to the internal stateful handler.
func (s *stateMachine) Run(taskID string) (done bool) {
	s.tid = taskID
	initial, err := s.ss.Load(taskID)
	if err != nil {
		// A failure to load the state for a task is *fatal* - the task will be
		// unscheduled and requires operator intervention to reschedule.
		metafora.Errorf("task=%q could not load initial state. Marking done! Error: %v", taskID, err)
		return true
	}
	s.mu = &sync.Mutex{}
	s.s = initial
	close(s.initialized)

	// Main Run loop
	done = false
	for {
		cur := s.State()
		switch cur.Code {
		case Runnable:
			done = s.h.Run(s, taskID)
		case Paused:
			//TODO Wait on a new message
			panic("not implemented")
		case Sleeping:
			//TODO Wait on command
			time.Sleep(cur.Until.Sub(time.Now()))
		case Error:
			//TODO check error history and possibly transition to Failed?
			panic("not implemented")
		case Completed, Failed, Killed:
			metafora.Infof("task=%q reached terminal state %s - exiting.", taskID, cur.Code)
			return true
		default:
			panic("invalid state: " + cur.Code.String())
		}
		return done
	}
}

func (s *stateMachine) run() bool {
	defer func() {
		if r := recover(); r != nil {
			//TODO Transition to Error?
		}
	}()
	return s.h.Run(s, s.tid)
}

// Stop applies a Release message to the state machine to cause the handler to
// exit but leave the task in a Runnable state.
func (s *stateMachine) Stop() {
	// Wait for the state machine to be initialized
	<-s.initialized
	if _, err := s.Apply(Message{Code: Release}); err != nil {
		if it, ok := err.(InvalidTransition); ok {
			metafora.Infof("task=%q couldn't be stopped: %s", s.tid, it)
		} else {
			metafora.Errorf("task=%q Unexpected error stopping: %v", s.tid, err)
		}
	}
}

// Apply a message to cause a state transition. InvalidTransition is returned
// with information about the current state since Apply can have concurrent
// callers. A caller should inspect InvalidTransition and if appropriate retry
// their state transition.
func (s *stateMachine) Apply(m Message) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg := m.Code
	cur := s.s.Code

	//XXX Is a linear scan of all rules really the best option here?
	for _, trans := range Rules {
		if trans.Event == msg && trans.From == cur {
			//XXX Should this really be done here?! And in a blocking fashion?!
			//    Asynchronously notifying the handler via a goroutine that catches panic()s
			if err := s.h.Transition(trans); err != nil {
				metafora.Errorf("Error from transition %s handling: %v", trans, err)
				return s.s, err
			}
			newstate := State{Code: trans.To, Until: m.Until}
			if err := s.ss.Store(s.tid, newstate); err != nil {
				metafora.Errorf("task=%q Error storing new state %s: %v", s.tid, newstate, err)
				return s.s, err
			}

			// Now that the state has been stored, use it and continue
			s.s = newstate
			metafora.Debugf("Transitioned %s", trans)
			return newstate, nil
		}
	}
	return s.s, InvalidTransition{Message: msg, Current: cur}
}

// State returns the current state. Terminal states will never change.
func (s *stateMachine) State() State {
	// Wait for the state machine to be initialized
	<-s.initialized
	s.mu.Lock()
	out := s.s
	s.mu.Unlock()
	return out
}
