package metafora

type Client interface {
	SubmitTask(taskId string) error

	//SubmitCommand(command string) error

	//QueryNodes() []NodeInfo
	//More to come!
}
