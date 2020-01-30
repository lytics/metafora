package statemachine_test

import (
	"errors"
	"testing"
	"time"

	. "github.com/lytics/metafora/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type task string

func (t task) ID() string { return string(t) }

func TestDefaultErrHandler(t *testing.T) {
	t.Parallel()
	tid := ""

	errs := []Err{{Time: time.Now()}}

	{
		msg, errs := DefaultErrHandler(task(tid), errs)
		if len(errs) != 1 {
			t.Fatalf("Expected 1 err, found: %d", len(errs))
		}
		if msg.Code != Sleep || msg.Until == nil || msg.Until.Before(time.Now().Add(9*time.Minute)) {
			t.Fatalf("Expected sleep until +10m state but found: %s", msg)
		}
	}

	// Push error list over limit
	for i := 0; i < DefaultErrMax+1; i++ {
		errs = append(errs, Err{Time: time.Now()})
	}

	{
		msg, errs := DefaultErrHandler(task(tid), errs)
		if len(errs) > DefaultErrMax {
			t.Fatalf("Expected %d errors but received: %d", DefaultErrMax, len(errs))
		}
		if msg.Code != Error || msg.Err != ExceededErrorRate {
			t.Fatalf("Expected error handler to permanently fail but receied: %s", msg)
		}
	}
}

type errType1 struct{ error }
type errType2 struct{ error }

func TestErr(t *testing.T) {
	err := errType1{errors.New("some underlying error")}
	se := NewErr(err, time.Now())

	// confirm se implements the error interface
	require.Implements(t, (*error)(nil), se)

	// confirm we can only convert se to an error of the same underlying type
	assert.True(t, errors.As(se, new(errType1)))
	assert.False(t, errors.As(se, new(errType2)))

	// make sure we don't panic if someone uses it the old way and baseErr is nil
	se = Err{Time: time.Now(), Err: "something bad"}
	assert.Equal(t, "something bad", se.Error())
	assert.False(t, errors.As(se, new(errType1)))
}
