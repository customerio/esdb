package cluster

import (
	"github.com/customerio/esdb"
	"github.com/customerio/esdb/stream"

	"github.com/bitly/go-simplejson"

	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func Compress(dbpath string, start, stop uint64, closed []uint64) error {
	commits := make([]uint64, 0, len(closed))

	for _, commit := range closed {
		if commit >= start && commit <= stop {
			commits = append(commits, commit)
		}
	}

	writer, err := esdb.New(filepath.Join(dbpath, "stream", fmt.Sprintf("events.%024v.esdb", start)))
	if err != nil {
		log.Fatal(err)
	}

	for _, commit := range commits {
		s, err := stream.Open(filepath.Join(dbpath, "stream", fmt.Sprintf("events.%024v.stream", commit)))
		if err != nil {
			log.Fatal(err)
		}

		s.Iterate(0, func(e *stream.Event) bool {
			js, err := simplejson.NewJson(e.Data)
			if err != nil {
				log.Fatal(err)
			}

			timestamp := js.Get("timestamp").MustInt()
			writer.Add([]byte("a"), e.Data, timestamp, "", e.Indexes())

			return true
		})
	}

	return writer.Write()
}

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
