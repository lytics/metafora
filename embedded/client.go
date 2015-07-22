package embedded

import "github.com/lytics/metafora"

func NewEmbeddedClient(taskchan chan metafora.Task, cmdchan chan *NodeCommand, nodechan chan []string) metafora.Client {
	return &EmbeddedClient{taskchan, cmdchan, nodechan}
}

type EmbeddedClient struct {
	taskchan chan<- metafora.Task
	cmdchan  chan<- *NodeCommand
	nodechan <-chan []string
}

func (ec *EmbeddedClient) SubmitTask(t metafora.Task) error {
	ec.taskchan <- t
	return nil
}

func (ec *EmbeddedClient) DeleteTask(taskid string) error {
	nodes, _ := ec.Nodes()
	// Simply submit stop for all nodes
	for _, nid := range nodes {
		ec.SubmitCommand(nid, metafora.CommandStopTask(taskid))
	}
	return nil
}

func (ec *EmbeddedClient) SubmitCommand(nodeid string, command metafora.Command) error {
	ec.cmdchan <- &NodeCommand{command, nodeid}
	return nil
}

func (ec *EmbeddedClient) Nodes() ([]string, error) {
	nodes := <-ec.nodechan
	return nodes, nil
}
