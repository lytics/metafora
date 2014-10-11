package metafora

import (
	"fmt"
	"log"
	"os"
)

var lflags = log.Ldate | log.Lmicroseconds | log.Lshortfile

// LogLevel specifies the severity of a given log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

type logOutputter interface {
	Output(calldepth int, s string) error
}

// Logger is the interface given to components.
type Logger interface {
	Log(LogLevel, string, ...interface{})
}

// LogPrefix Resolution
func (lvl LogLevel) String() (name string) {
	switch lvl {
	case LogLevelDebug:
		name = "DEBUG"
	case LogLevelInfo:
		name = "INFO"
	case LogLevelWarn:
		name = "WARN"
	case LogLevelError:
		name = "ERROR"
	}
	return
}

// logger is the base logging struct that handles writing to the logOutputter.
type logger struct {
	l   logOutputter
	lvl LogLevel
}

func stdoutLogger() *logger {
	return &logger{l: log.New(os.Stdout, "", lflags), lvl: LogLevelInfo}
}

func (l *logger) Log(lvl LogLevel, msg string, args ...interface{}) {
	if l.l == nil {
		return
	}

	if l.lvl > lvl {
		return
	}

	l.l.Output(2, fmt.Sprintf("[%s] %s", lvl, fmt.Sprintf(msg, args...)))
}
