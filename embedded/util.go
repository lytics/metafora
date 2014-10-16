package embedded

import "github.com/lytics/metafora"

type NodeCommand struct {
	Cmd    metafora.Command
	NodeId string
}

// Returns a connected client/coordinator pair for embedded/testing use
func NewEmbeddedPair(nodeid string) (metafora.Coordinator, metafora.Client) {
	taskchan := make(chan string)
	cmdchan := make(chan *NodeCommand)
	nodechan := make(chan []string)

	coord := NewEmbeddedCoordinator(nodeid, taskchan, cmdchan, nodechan)
	client := NewEmbeddedClient(taskchan, cmdchan, nodechan)

	return coord, client
}
