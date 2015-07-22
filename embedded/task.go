package embedded

// Task is the embedded coorindator's metafora.Task implemenation.
type Task struct {
	TID string
}

func (t *Task) ID() string { return t.TID }
