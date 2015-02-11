package resreporter

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

const meminfo = "/proc/meminfo"

var Memory = memory{}

type memory struct{}

func (memory) Used() (used uint64, total uint64) {
	fd, err := os.Open(meminfo)
	if err != nil {
		//XXX Should this use metafora's logger somehow?
		log.Printf("Error reading free memory via "+meminfo+": %v", err)

		// Effectively disable the balancer since an error happened
		return 0, 100
	}
	defer fd.Close()

	s := bufio.NewScanner(fd)
	foundFree, foundCache, foundBuf := false, false, false
	var cache uint64
	var buffered uint64
	var free uint64
	for s.Scan() {
		if total == 0 {
			if n, _ := fmt.Sscanf(s.Text(), "MemTotal:%d", &total); n == 1 {
				continue
			}
		}
		if foundFree {
			if n, _ := fmt.Sscanf(s.Text(), "MemFree:%d", &free); n == 1 {
				continue
			}
		}
		if !foundCache {
			if n, _ := fmt.Sscanf(s.Text(), "Cached:%d", &cache); n == 1 {
				foundCache = true
				continue
			}
		}
		if !foundBuf {
			if n, _ := fmt.Sscanf(s.Text(), "Buffers:%d", &buffered); n == 1 {
				foundBuf = true
				continue
			}
		}
	}
	if err := s.Err(); err != nil {
		//XXX Should this use metafora's logger somehow?
		log.Printf("Error reading free memory via "+meminfo+": %v", err)

		// Effectively disable the balancer since an error happened
		return 0, 100
	}

	return total - (free + buffered + cache), total
}

func (memory) String() string { return "kB" }
