package cluster

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func Cleanup(dbpath string, current uint64, closed []uint64) error {
	commits := map[uint64]bool{current: true}

	for _, commit := range closed {
		commits[commit] = true
	}

	return filepath.Walk(filepath.Join(dbpath, "stream"), func(path string, f os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".stream") {
			return nil
		}

		part := strings.Replace(path, filepath.Join(dbpath, "stream", "events."), "", 1)
		part = strings.Replace(part, ".stream", "", 1)

		commit, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return err
		}

		if !commits[uint64(commit)] && uint64(commit) < current {
			return os.Remove(path)
		}

		return nil
	})
}
