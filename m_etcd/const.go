package m_etcd

const (
	TasksPath     = "tasks"
	NodesPath     = "nodes"
	CommandsPath  = "commands"
	CreatedMarker = "created.info"
	OwnerMarker   = "owner"
	ClaimTTL      = 120   //seconds
	NodeIDTTL     = 86400 // 24 hours
	ForeverTTL    = 0
)
