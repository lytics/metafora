package metafora

import (
	"github.com/araddon/gou"
)

var LogLevel int = gou.LogLevel

type LogOutputter interface {
	Output(calldepth int, s string) error
}

// SetLogger switches where Metafora logs.
func SetLogger(l LogOutputter) {
}

var Debug func(v ...interface{}) = gou.Debug
var Debugf func(format string, v ...interface{}) = gou.Debugf
var Info func(v ...interface{}) = gou.Info
var Infof func(format string, v ...interface{}) = gou.Infof
var Warn func(v ...interface{}) = gou.Warn
var Warnf func(format string, v ...interface{}) = gou.Warnf
var Error func(v ...interface{}) = gou.Error
var Errorf func(format string, v ...interface{}) = gou.Errorf
