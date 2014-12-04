package metafora_test

import (
	"reflect"
	"testing"

	. "github.com/lytics/metafora"
)

func testCmd(t *testing.T, cmd Command, name string, params map[string]interface{}) {
	if cmd.Name() != name {
		t.Errorf("%s command's name is wrong: %s", name, cmd.Name())
	}
	if !reflect.DeepEqual(cmd.Parameters(), params) {
		t.Errorf("%s command's params are wrong. expected %#v != %#v", name, params, cmd.Parameters())
	}
	b, err := cmd.Marshal()
	if err != nil {
		t.Errorf("%s command's Marshal() returned an error: %v", err)
		return
	}
	cmd2, err := UnmarshalCommand(b)
	if err != nil {
		t.Errorf("%s command's Marshal() output could not be Unmarshalled: %v", name, err)
		return
	}
	if cmd2.Name() != name {
		t.Errorf("%s command's name didn't Unmarshal properly: %s", cmd2.Name())
	}
	if !reflect.DeepEqual(cmd2.Parameters(), params) {
		t.Errorf("%s command's params didn't Unmarshal properly. expected %#v != %#v",
			name, params, cmd2.Parameters())
	}
}

func TestCommands(t *testing.T) {
	t.Parallel()
	testCmd(t, CommandFreeze(), "freeze", nil)
	testCmd(t, CommandUnfreeze(), "unfreeze", nil)
	testCmd(t, CommandBalance(), "balance", nil)
	testCmd(t, CommandReleaseTask("test"), "release_task", map[string]interface{}{"task": "test"})
	testCmd(t, CommandStopTask("test"), "stop_task", map[string]interface{}{"task": "test"})
}
