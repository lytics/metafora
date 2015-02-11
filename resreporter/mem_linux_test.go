package resreporter_test

import (
	"testing"

	"github.com/lytics/metafora/resreporter"
)

func TestMemReporter(t *testing.T) {
	used, total := resreporter.Memory.Used()
	t.Logf("Used: %d %s (%d mB)", used, resreporter.Memory, used/1024)
	t.Logf("Total: %d %s (%d mB)", total, resreporter.Memory, total/1024)
	if used == 0 && total == 100 {
		t.Fatal("Memory reporter failed!")
	}
	if used > total {
		t.Fatal("More memory used than available?!")
	}
}
