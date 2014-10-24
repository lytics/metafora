package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

var (
	NoArgs = errors.New("koalemosd: no args in task")
)

type shellHandler struct {
	etcdc *etcd.Client
	id    string
	m     sync.Mutex
	p     *os.Process
	ps    *os.ProcessState
	stop  bool
}

// Run retrieves task information from etcd and executes it.
func (h *shellHandler) Run(taskID string) error {
	h.id = taskID

	const sort, recurs = false, false
	resp, err := h.etcdc.Get("/koalemos-tasks/"+taskID, sort, recurs)
	if err != nil {
		h.log("Failed retrieving task from etcd: %v", err)
		return err
	}

	task := struct{ Args []string }{}
	if err := json.Unmarshal([]byte(resp.Node.Value), &task); err != nil {
		h.log("Failed to unmarshal command body: %v", err)
		return err
	}
	if len(task.Args) == 0 {
		h.log("No Args in task: %s", resp.Node.Value)
		return NoArgs
	}

	cmd := exec.Command(task.Args[0], task.Args[1:]...)

	// Set stdout and stderr to temporary files
	stdout, stderr, err := outFiles(taskID)
	if err != nil {
		h.log("Could not create log files: %v", err)
		return err
	}
	defer stdout.Close()
	defer stderr.Close()

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	// Entering critical section where we have to lock handler fields to avoid
	// race conditions with Stop() getting called.
	h.m.Lock()
	if h.stop {
		h.log("Task stopped before it even started.")
		h.m.Unlock()
		return nil
	}

	h.log("Running task: %s", strings.Join(task.Args, " "))
	if err := cmd.Start(); err != nil {
		h.m.Unlock()
		h.log("Error starting task: %v", err)
		return nil // don't return the error, metafora doesn't care
	}
	h.p = cmd.Process
	h.ps = cmd.ProcessState

	// Leaving critical section. Now if Stop() is called, cmd.Wait() will return.
	h.m.Unlock()

	h.log("running")
	stopping := false

	if err := cmd.Wait(); err != nil {
		if err.(*exec.ExitError).Sys().(syscall.WaitStatus).Signal() == os.Interrupt {
			stopping = true
		} else {
			// Metafora doesn't care about internal task failures, so just log it
			h.log("Exited with error: %v", err)
		}
	}

	// Only delete task if we're not stopping
	if !stopping {
		//FIXME Use CompareAndDelete
		if _, err := h.etcdc.Delete("/koalemos-tasks/"+taskID, recurs); err != nil {
			h.log("Error deleting task body: %v", err)
		}
	}
	h.log("done")
	return nil
}

// Stop sends the Interrupt signal to the running process.
func (h *shellHandler) Stop() {
	h.m.Lock()
	defer h.m.Unlock()

	h.log("Setting as stopped")
	h.stop = true

	if h.p != nil && h.ps != nil && !h.ps.Exited() {
		h.log("Process has not started.")
		return
	}

	if err := h.p.Signal(os.Interrupt); err != nil {
		h.log("Error stopping process %d: %v", h.p.Pid, err)
	}
}

func (h *shellHandler) log(msg string, v ...interface{}) {
	log.Printf("[%s] %s", h.id, fmt.Sprintf(msg, v...))
}

func outFiles(name string) (io.WriteCloser, io.WriteCloser, error) {
	stdout, err := os.Create(filepath.Join(os.TempDir(), name+"-stdout.log"))
	if err != nil {
		return nil, nil, err
	}
	stderr, err := os.Create(filepath.Join(os.TempDir(), name+"-stderr.log"))
	return stdout, stderr, err
}

func makeHandlerFunc(c *etcd.Client) metafora.HandlerFunc {
	return func() metafora.Handler {
		return &shellHandler{etcdc: c}
	}
}
