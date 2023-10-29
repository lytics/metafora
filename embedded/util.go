package embedded

import "github.com/lytics/metafora"

type NodeCommand struct {
	Cmd    metafora.Command
	NodeId string
}

// NewEmbeddedPair returns a connected client/coordinator pair for embedded/testing use
func NewEmbeddedPair(nodeid string) (metafora.Coordinator, metafora.Client) {
	taskchan := make(chan metafora.Task)
	cmdchan := make(chan *NodeCommand)
	nodechan := make(chan []string, 1)

	coord := NewEmbeddedCoordinator(nodeid, taskchan, cmdchan, nodechan)
	client := NewEmbeddedClient(taskchan, cmdchan, nodechan)

	return coord, client
}
