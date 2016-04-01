package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/lytics/metafora/examples/koalemos"
)

type shellHandler struct {
	task *koalemos.Task
	m    sync.Mutex
	p    *os.Process
	ps   *os.ProcessState
	stop bool
}

// Run retrieves task information from etcd and executes it.
func (h *shellHandler) Run() (done bool) {
	const sort, recurs = false, false
	if len(h.task.Args) == 0 {
		h.log("No Args in task")
		return true
	}

	cmd := exec.Command(h.task.Args[0], h.task.Args[1:]...)

	// Set stdout and stderr to temporary files
	stdout, stderr, err := outFiles(h.task.ID())
	if err != nil {
		h.log("Could not create log files: %v", err)
		return false
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
		return false
	}

	h.log("Running task: %s", strings.Join(h.task.Args, " "))
	if err := cmd.Start(); err != nil {
		h.m.Unlock()
		h.log("Error starting task: %v", err)
		return true
	}
	h.p = cmd.Process
	h.ps = cmd.ProcessState

	// Leaving critical section. Now if Stop() is called, cmd.Wait() will return.
	h.m.Unlock()

	h.log("running")

	if err := cmd.Wait(); err != nil {
		if err.(*exec.ExitError).Sys().(syscall.WaitStatus).Signal() == os.Interrupt {
			h.log("Stopping")
			// Not done!
			done = false
		} else {
			h.log("Exited with error: %v", err)
			done = true // don't retry commands that error'd
		}
	} else {
		done = true
	}

	h.log("done? %t", done)
	return done
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
	log.Printf("[%s] %s", h.task.ID(), fmt.Sprintf(msg, v...))
}

func outFiles(name string) (io.WriteCloser, io.WriteCloser, error) {
	stdout, err := os.Create(filepath.Join(os.TempDir(), name+"-stdout.log"))
	if err != nil {
		return nil, nil, err
	}
	stderr, err := os.Create(filepath.Join(os.TempDir(), name+"-stderr.log"))
	return stdout, stderr, err
}
