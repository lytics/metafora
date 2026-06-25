package metcdv3

import "time"

const (
	// resyncBackoffMin is the initial delay before re-syncing a watch after a
	// compaction; resyncBackoffMax is the ceiling for exponential growth. The
	// delay throttles the re-Get/re-watch loop so a pathological compaction
	// storm can't hammer etcd while it's already unhealthy (the condition
	// commonly coincides with member restarts/relocations).
	resyncBackoffMin = 500 * time.Millisecond
	resyncBackoffMax = 30 * time.Second

	// resyncHealthyThreshold is how long a watch must run before a subsequent
	// compaction is treated as a fresh, isolated event (resetting the backoff)
	// rather than part of a tight failure loop.
	resyncHealthyThreshold = 30 * time.Second

	// resyncEscalateAfter is the number of consecutive rapid resyncs after
	// which we give up re-syncing and fall back to the original behavior of
	// surfacing the compaction error (panicking the consumer loop / faulting
	// the task). A single compaction always recovers via a fresh Get, so
	// reaching this threshold means resyncs are failing in a tight loop, i.e.
	// etcd is persistently unhealthy and the error should be surfaced.
	resyncEscalateAfter = 5
)

// resyncBackoff tracks exponential backoff between watch resync attempts. It is
// not safe for concurrent use; each watch goroutine owns its own.
type resyncBackoff struct {
	cur         time.Duration
	consecutive int
}

// next records a resync that followed a watch which ran for watchedFor and
// returns how long to wait before retrying and whether the situation has
// escalated (too many rapid resyncs in a row). A watch that ran longer than
// resyncHealthyThreshold resets the backoff, so an isolated compaction always
// incurs only the minimum delay.
func (b *resyncBackoff) next(watchedFor time.Duration) (delay time.Duration, escalated bool) {
	if watchedFor >= resyncHealthyThreshold {
		b.cur = 0
		b.consecutive = 0
	}

	switch {
	case b.cur < resyncBackoffMin:
		b.cur = resyncBackoffMin
	default:
		b.cur *= 2
		if b.cur > resyncBackoffMax {
			b.cur = resyncBackoffMax
		}
	}
	b.consecutive++

	return b.cur, b.consecutive >= resyncEscalateAfter
}
