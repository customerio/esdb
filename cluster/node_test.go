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

func trackevent(n *Node, data []byte, indexes map[string]string) {
	n.Event(data, indexes)
}

func TestScanningWithoutRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(100000000)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Scan("a", "b", 0, "", func(e *stream.Event) bool {
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

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Scan("a", "b", 0, "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"c", "b", "a"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}

func TestScanningAfterWithRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Scan("a", "b", 2, "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"c", "b"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b"}, found)
		}
	})
}

func TestIteratingWithoutRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(100000000)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Iterate(0, "", func(e *stream.Event) bool {
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

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Iterate(0, "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"a", "b", "c"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}

func TestIteratingAfterWithRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.Iterate(2, "", func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"b", "c"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"b", "c"}, found)
		}
	})
}
