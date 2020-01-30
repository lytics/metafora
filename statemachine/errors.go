package statemachine

import (
	"errors"
	"time"

	"github.com/lytics/metafora"
)

// ExceededErrorRate is returned by error handlers in an Error Message when
// retry logic has been exhausted for a handler and it should transition to
// Failed.
var ExceededErrorRate = errors.New("exceeded error rate")

// Err represents an error that occurred while a stateful handler was running.
//
// NewErr was added to allow callers to construct an instance from an underlying error.
// The underlying error is now preserved so that Err can be converted back using errors.As
// This is useful for custom error handlers that wish to inspect underlying error types
// and decision accordingly.
type Err struct {
	Time    time.Time `json:"timestamp"`
	Err     string    `json:"error"`
	baseErr error
}

// NewErr constructs an Err from an underlying error e
func NewErr(e error, t time.Time) Err {
	return Err{Err: e.Error(), Time: t, baseErr: e}
}

// Error implements the Error interface.
func (e Err) Error() string {
	return e.Err
}

// As implements the error interface for Err. This allows an instance of Err to be
// converted back to its underlying error type using errors.As
func (e Err) As(target interface{}) bool {
	return errors.As(e.baseErr, target)
}

// ErrHandler functions should return Run, Sleep, or Fail messages depending on
// the rate of errors.
//
// Either ErrHandler and/or StateStore should trim the error slice to keep it
// from growing without bound.
type ErrHandler func(task metafora.Task, errs []Err) (*Message, []Err)

const (
	DefaultErrLifetime = -4 * time.Hour
	DefaultErrMax      = 8
)

// DefaultErrHandler returns a Fail message if 8 errors have occurred in 4
// hours. Otherwise it enters the Sleep state for 10 minutes before trying
// again.
func DefaultErrHandler(_ metafora.Task, errs []Err) (*Message, []Err) {
	recent := time.Now().Add(DefaultErrLifetime)
	strikes := 0
	for _, err := range errs {
		if err.Time.After(recent) {
			strikes++
		}
	}

	if len(errs) > DefaultErrMax {
		errs = errs[len(errs)-DefaultErrMax:]
	}

	if strikes >= DefaultErrMax {
		// Return a new error to transition to Failed as well as the original
		// errors to store what caused this failure.
		return ErrorMessage(ExceededErrorRate), errs
	}
	return SleepMessage(time.Now().Add(10 * time.Minute)), errs
}
