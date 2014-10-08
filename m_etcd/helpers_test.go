package m_etcd

import (
	"fmt"
	"os"
	"testing"

	"github.com/lytics/metafora"
)

func skipEtcd(t *testing.T) {
	if os.Getenv("ETCDTESTS") == "" {
		t.Skip("ETCDTESTS unset. Skipping etcd tests.")
	}
}

type testLogger struct {
	prefix string
	*testing.T
}

func (l testLogger) Log(lvl metafora.LogLevel, m string, v ...interface{}) {
	l.T.Log(fmt.Sprintf("%s:[%s] %s", l.prefix, lvl, fmt.Sprintf(m, v...)))
}
