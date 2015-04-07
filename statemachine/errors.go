package statemachine

import "fmt"

type InvalidTransition struct {
	Message MessageCode
	Current StateCode
}

func (e InvalidTransition) Error() string {
	return fmt.Sprintf("could not transition from %s with message %s", e.Current, e.Message)
}
