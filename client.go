package metafora

type Client interface {
	// SubmitTask submits a task to the system, the task id must be unique.
	SubmitTask(taskId string) error

	// Delete a task
	DeleteTask(taskId string) error

	// SubmitCommand submits a command to a particular node.
	SubmitCommand(nodeId string, command string) error

	// Nodes retrieves the current set of registered nodes.
	Nodes() ([]string, error)
}
