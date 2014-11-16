package cluster

import (
	"github.com/customerio/esdb/stream"

	"os"
	"reflect"
	"testing"
	"time"
)

func withNode(perform func(n *Node)) {
	os.RemoveAll("tmp")
	os.MkdirAll("tmp", 0755)

	node := NewNode("tmp/teststream", "localhost", 3001)
	go node.Start("")

	for node.raft == nil || !node.raft.Running() {
		time.Sleep(5 * time.Millisecond)
	}

	perform(node)

	node.Stop()
}

func TestScanningWithoutRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(100000000)

		n.Event([]byte("a"), map[string]string{"a": "b"})
		n.Event([]byte("b"), map[string]string{"a": "b"})
		n.Event([]byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Scan("a", "b", "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"c", "b", "a"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}

func TestScanningWithRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)

		n.Event([]byte("a"), map[string]string{"a": "b"})
		n.Event([]byte("b"), map[string]string{"a": "b"})
		n.Event([]byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Scan("a", "b", "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"c", "b", "a"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}

func TestIteratingWithoutRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(100000000)

		n.Event([]byte("a"), map[string]string{"a": "b"})
		n.Event([]byte("b"), map[string]string{"a": "b"})
		n.Event([]byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Iterate("", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"a", "b", "c"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}

func TestIteratingWithRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)

		n.Event([]byte("a"), map[string]string{"a": "b"})
		n.Event([]byte("b"), map[string]string{"a": "b"})
		n.Event([]byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Iterate("", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"a", "b", "c"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}
