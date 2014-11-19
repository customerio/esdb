package cluster

import (
	"github.com/customerio/esdb/stream"

	"fmt"
	"path/filepath"
)

func Merge(dbpath string, start, stop uint64, closed []uint64) error {
	paths := make([]string, 0, len(closed))

	for _, commit := range closed {
		if commit >= start && commit <= stop {
			paths = append(paths, filepath.Join(dbpath, "stream", fmt.Sprintf("events.%024v.stream", commit)))
		}
	}

	return stream.Merge(filepath.Join(dbpath, "stream", fmt.Sprintf("events.%024v.tmpstream", start)), paths)
}
