package m_etcd

import "time"

const (
	DefaultClaimTTL = 3 * time.Minute
	DefaultNodeTTL  = 1 * time.Minute

	TasksPath    = "tasks"
	NodesPath    = "nodes"
	CommandsPath = "commands"
	MetadataKey  = "_metafora" // _{KEYs} are hidden files, so this will not trigger our watches
	OwnerMarker  = "owner"
	PropsKey     = "props"
)
