package metafora

import "encoding/json"

const (
	cmdFreeze   = "freeze"
	cmdUnfreeze = "unfreeze"
	cmdBalance  = "balance"
	cmdStopTask = "stop_task"
)

// Commands are a way clients can communicate directly with nodes for cluster
// maintenance.
//
// Use the Command functions to generate implementations of this interface.
// Metafora's consumer will discard unknown commands.
type Command interface {
	// Name returns the name of the command.
	Name() string

	// Parameters returns the parameters, if any, the command will be executed
	// with.
	Parameters() map[string]interface{}

	// Marshal turns a command into its wire representation.
	Marshal() ([]byte, error)
}

// command is the internal representation of commands used for serialization.
type command struct {
	C string                 `json:"command"`
	P map[string]interface{} `json:"parameters,omitempty"`
}

// Name returns the name of the command.
func (c *command) Name() string {
	return c.C
}

// Parameters returns the parameters, if any, the command will be executed
// with.
func (c *command) Parameters() map[string]interface{} {
	return c.P
}

// Marshal turns a command into its wire representation.
func (c *command) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Unmarshal parses a command from its wire representation.
func UnmarshalCommand(p []byte) (Command, error) {
	c := &command{}
	err := json.Unmarshal(p, c)
	return c, err
}

// CommandFreeze stops all task watching and balancing.
func CommandFreeze() Command {
	return &command{C: cmdFreeze}
}

// CommandUnfreeze resumes task watching and balancing.
func CommandUnfreeze() Command {
	return &command{C: cmdUnfreeze}
}

// CommandBalance forces the node's balancer.Balance method to be called even
// if frozen.
func CommandBalance() Command {
	return &command{C: cmdBalance}
}

// CommandStopTask forces a node to stop a task even if frozen.
func CommandStopTask(task string) Command {
	return &command{C: cmdStopTask, P: map[string]interface{}{"task": task}}
}
