package statemachine

type CommandListener interface {
	Receive() <-chan Message
	Stop()
}

type Commander interface {
	Send(taskID string, m Message) error
}
