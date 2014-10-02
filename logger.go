package metafora

import "fmt"

// logging constants
const (
	LogLevelDebugPrefix   = "DEBUG"
	LogLevelInfoPrefix    = "INFO"
	LogLevelWarningPrefix = "WARN"
	LogLevelErrorPrefix   = "ERROR"
)

// LogLevel specifies the severity of a given log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

type logOutputter interface {
	Output(calldepth int, s string) error
}

// Logger is the interface given to components.
type Logger interface {
	Log(LogLevel, string, ...interface{})
	Level() LogLevel
}

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

// logger is the base logging struct that handles writing to the logOutputter.
type logger struct {
	l   logOutputter
	lvl LogLevel
}

func (l *logger) Log(lvl LogLevel, msg string, args ...interface{}) {
	if l.l == nil {
		return
	}

	if l.lvl > lvl {
		return
	}

	l.l.Output(2, fmt.Sprintf("[%s] %s", logPrefix(lvl), fmt.Sprintf(msg, args...)))
}

func (l *logger) Level() LogLevel {
	return l.lvl
}

// prefixLogger wraps a logger and prepends a prefix to all output.
type prefixLogger struct {
	Logger
	p string
}

func newPrefixLogger(l Logger, p string) Logger {
	return &prefixLogger{Logger: l, p: p}
}

func (l *prefixLogger) Log(lvl LogLevel, msg string, args ...interface{}) {
	newArgs := make([]interface{}, len(args)+1)
	newArgs[0] = l.p
	copy(newArgs[1:], args)
	l.Logger.Log(lvl, "%s %s", newArgs...)
}
