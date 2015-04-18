package cluster

import (
	"os"
	"os/exec"

	"github.com/customerio/esdb/stream"

	"fmt"
	"path/filepath"
)

func Clear(dbpath string, stop uint64, closed []uint64) error {
	tmp, err := stream.New("tmp.stream")
	if err != nil {
		return err
	}

	if err = tmp.Close(); err != nil {
		return err
	}

	for _, commit := range closed {
		fmt.Println("commit", commit)
		if commit < stop {
			path := filepath.Join(dbpath, "stream", fmt.Sprintf("events.%024v.stream", commit))
			fmt.Println("clearing", path)
			err := exec.Command("cp", "-f", "tmp.stream", path).Run()
			if err != nil {
				return err
			}
		}
	}

	os.Remove("tmp.stream")

	return nil
}
