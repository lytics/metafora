package statemachine_test

import (
	"testing"
	"time"

	. "github.com/lytics/metafora/statemachine"
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
