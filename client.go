package metafora

type Client interface {
	// Submit a task to the system, the task id must be unique.
	SubmitTask(taskId string) error

	// Submit a command to a particular node.
	SubmitCommand(nodeId string, command string) error

	// Retrieve the current set of registered nodes.
	Nodes() ([]string, error)
}
