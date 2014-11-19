package cluster

import (
	"github.com/customerio/esdb/stream"

	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func streamCommits() []int {
	commits := make([]int, 0)

	err := filepath.Walk("tmp/teststream", func(path string, f os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".stream") {
			return nil
		}

		path = strings.Replace(path, "tmp/teststream/stream/events.", "", 1)
		path = strings.Replace(path, ".stream", "", 1)

		commit, err := strconv.ParseInt(path, 10, 64)
		if err != nil {
			panic(err)
		}

		commits = append(commits, int(commit))

		return nil
	})

	if err != nil {
		panic(err)
	}

	return commits
}

func TestCompression(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)
		n.SetSnapshotBuffer(1000)

		expectedCommits := []int{1}

		for i := 0; i < 500; i++ {
			expectedCommits = append(expectedCommits, i+3)
			trackevent(n, []byte(strconv.Itoa(i)), map[string]string{"a": "b"})
		}

		if !reflect.DeepEqual(streamCommits(), expectedCommits) {
			t.Errorf("Incorrect committed streams. Wanted: %v, found: %v", expectedCommits, streamCommits())
		}

		Merge("tmp/teststream", 1, 291, n.db.closed)
		n.Compress(1, 291)
		Merge("tmp/teststream", 292, 490, n.db.closed)
		n.Compress(292, 490)
		Cleanup("tmp/teststream", n.db.current, n.db.closed)

		expectedCommits = []int{1, 292, 491, 492, 493, 494, 495, 496, 497, 498, 499, 500, 501, 502}

		// Test that we have 2 large streams and 10 normal stream files
		if !reflect.DeepEqual(streamCommits(), expectedCommits) {
			t.Errorf("Incorrect post-compress committed streams. Wanted: %v, found: %v", expectedCommits, streamCommits())
		}

		found := make([]string, 0)

		_, err := n.db.Scan("a", "b", "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if err != nil {
			t.Errorf("Error iterating results: %v", err)
		}

		expected := make([]string, 0)
		for i := 0; i < 500; i++ {
			expected = append(expected, strconv.Itoa(499-i))
		}

		if !reflect.DeepEqual(found, expected) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", expected, found)
		}

		found = make([]string, 0)

		_, err = n.db.Iterate("", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if err != nil {
			t.Errorf("Error iterating results: %v", err)
		}

		expected = make([]string, 0)
		for i := 0; i < 500; i++ {
			expected = append(expected, strconv.Itoa(i))
		}

		if !reflect.DeepEqual(found, expected) {
			t.Errorf("Incorrect iterate results. Wanted: %v, found: %v", expected, found)
		}
	})
}
