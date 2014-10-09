package m_etcd

const (
	TasksPath     = "tasks"
	NodesPath     = "nodes"
	CommandsPath  = "commands"
	CreatedMarker = "_created.info" // Hidden file
	OwnerMarker   = "owner"
	ClaimTTL      = 120   //seconds
	NodeIDTTL     = 86400 // 24 hours
	ForeverTTL    = 0     //Ref: https://github.com/coreos/go-etcd/blob/e10c58ee110f54c2f385ac99764e8a7ca4cb13df/etcd/requests.go#L356
)
