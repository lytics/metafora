package metcdv3

const (
	TasksPath    = "tasks"
	NodesPath    = "nodes"
	CommandsPath = "commands"
	// Is this true for etcdv3?
	MetadataPath = "_metafora" // _{KEYs} are hidden files, so this will not trigger our watches
	OwnerPath    = "owner"
	PropsPath    = "props"

	//Etcd Error codes are passed directly through go-etcd from the http response,
	//So to find the error codes use this ref:
	//       https://go.etcd.io/etcd/blob/master/error/error.go#L67
	EcodeKeyNotFound   = 100
	EcodeCompareFailed = 101
	EcodeNodeExist     = 105
	EcodeExpiredIndex  = 401 // The event in requested index is outdated and cleared
)
