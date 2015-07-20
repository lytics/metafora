package metafora

type Client interface {
	// SubmitTask submits a task to the system, the task id must be unique.
	SubmitTask(Task) error

	// Delete a task
	DeleteTask(taskId string) error

	// SubmitCommand submits a command to a particular node.
	SubmitCommand(node string, command Command) error

	// Nodes retrieves the current set of registered nodes.
	Nodes() ([]string, error)
}
