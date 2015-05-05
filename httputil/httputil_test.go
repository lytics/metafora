package httputil_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lytics/metafora"
	. "github.com/lytics/metafora/httputil"
)

type tc struct {
	stop chan bool
}

func (*tc) Init(metafora.CoordinatorContext) error { return nil }
func (c *tc) Watch(chan<- string) error {
	<-c.stop
	return nil
}
func (c *tc) Claim(string) bool { return false }
func (c *tc) Release(string)    {}
func (c *tc) Done(string)       {}
func (c *tc) Command() (metafora.Command, error) {
	<-c.stop
	return nil, nil
}
func (c *tc) Close()       { close(c.stop) }
func (c *tc) Name() string { return "tc" }

func TestMakeInfoHandler(t *testing.T) {
	t.Parallel()

	c, _ := metafora.NewConsumer(&tc{stop: make(chan bool)}, nil, metafora.DumbBalancer)
	defer c.Shutdown()
	now := time.Now().Truncate(time.Second)

	resp := httptest.NewRecorder()
	MakeInfoHandler(c, now)(resp, nil)

	info := InfoResponse{}
	if err := json.Unmarshal(resp.Body.Bytes(), &info); err != nil {
		t.Fatalf("Error unmarshalling response body: %v", err)
	}
	if info.Frozen {
		t.Errorf("Consumer should not start frozen.")
	}
	if !info.Started.Equal(now) {
		t.Errorf("Started time %s != %s", info.Started, now)
	}
	if info.Name != "tc" {
		t.Errorf("Node name %q != tc", info.Name)
	}
	if len(info.Tasks) != 0 {
		t.Errorf("Unexpected tasks: %v", info.Tasks)
	}
}
