package metafora

// LogLevel specifies the severity of a given log message
type LogLevel int

type logger interface {
	Output(calldepth int, s string) error
}

// logging constants
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError

	LogLevelDebugPrefix   = "DEBUG"
	LogLevelInfoPrefix    = "INFO"
	LogLevelWarningPrefix = "WARN"
	LogLevelErrorPrefix   = "ERROR"
)

// LogPrefix Resolution
func logPrefix(lvl LogLevel) string {
	var prefix string

	switch lvl {
	case LogLevelDebug:
		prefix = LogLevelDebugPrefix
	case LogLevelInfo:
		prefix = LogLevelInfoPrefix
	case LogLevelWarning:
		prefix = LogLevelWarningPrefix
	case LogLevelError:
		prefix = LogLevelErrorPrefix
	}

	return prefix
}
