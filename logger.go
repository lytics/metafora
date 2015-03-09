package metafora

import (
	"fmt"
	"log"
	"os"
	"sync"
)

var std = &logger{l: DefaultLogger(), lvl: LogLevelInfo}

// LogLevel specifies the severity of a given log message
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

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
	default:
		name = "invalid"
	}
	return
}

func Debug(v ...interface{})                 { std.log(LogLevelDebug, v...) }
func Debugf(format string, v ...interface{}) { std.logf(LogLevelDebug, format, v...) }
func Info(v ...interface{})                  { std.log(LogLevelInfo, v...) }
func Infof(format string, v ...interface{})  { std.logf(LogLevelInfo, format, v...) }
func Warn(v ...interface{})                  { std.log(LogLevelWarn, v...) }
func Warnf(format string, v ...interface{})  { std.logf(LogLevelWarn, format, v...) }
func Error(v ...interface{})                 { std.log(LogLevelError, v...) }
func Errorf(format string, v ...interface{}) { std.logf(LogLevelError, format, v...) }

// LogOutputter is an interface representing *log.Logger instances so users
// aren't forced to use a *log.Logger.
type LogOutputter interface {
	Output(calldepth int, s string) error
}

// DefaultLogger returns the default *log.Logger used by Metafora.
func DefaultLogger() LogOutputter {
	return log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)
}

// SetLogger switches where Metafora logs.
func SetLogger(l LogOutputter) {
	std.mu.Lock()
	std.l = l
	std.mu.Unlock()
}

// SetLogLevel sets the log level (if it's valid) and returns the previous level.
func SetLogLevel(lvl LogLevel) LogLevel {
	std.mu.Lock()
	defer std.mu.Unlock()
	if lvl.String() == "invalid" {
		return std.lvl
	}
	old := std.lvl
	std.lvl = lvl
	return old
}

type logger struct {
	mu  sync.Mutex
	l   LogOutputter
	lvl LogLevel
}

func (l *logger) log(lvl LogLevel, v ...interface{}) {
	if l.l == nil {
		return
	}

	if l.lvl > lvl {
		return
	}

	l.l.Output(3, fmt.Sprintf("[%s] %s", lvl, fmt.Sprint(v...)))
}

func (l *logger) logf(lvl LogLevel, format string, v ...interface{}) {
	if l.l == nil {
		return
	}

	if l.lvl > lvl {
		return
	}

	l.l.Output(3, fmt.Sprintf("[%s] %s", lvl, fmt.Sprintf(format, v...)))
}
