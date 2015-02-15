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

func TestScanAllWithoutRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(100000000)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.ScanAll("a", "b", 0, func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"c", "b", "a"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b", "a"}, found)
		}
	})
}

func TestScanAllWithRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(30)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})
		trackevent(n, []byte("d"), map[string]string{"a": "b"})
		trackevent(n, []byte("e"), map[string]string{"a": "b"})
		trackevent(n, []byte("f"), map[string]string{"a": "b"})
		trackevent(n, []byte("g"), map[string]string{"a": "b"})
		trackevent(n, []byte("h"), map[string]string{"a": "b"})
		trackevent(n, []byte("i"), map[string]string{"a": "b"})
		trackevent(n, []byte("j"), map[string]string{"a": "b"})
		trackevent(n, []byte("k"), map[string]string{"a": "b"})
		trackevent(n, []byte("l"), map[string]string{"a": "b"})
		trackevent(n, []byte("m"), map[string]string{"a": "b"})
		trackevent(n, []byte("n"), map[string]string{"a": "b"})
		trackevent(n, []byte("o"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.ScanAll("a", "b", 0, func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"o", "n", "m", "l", "j", "h", "f", "d", "b", "k", "i", "g", "e", "c", "a"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"o", "n", "m", "l", "j", "h", "f", "d", "b", "k", "i", "g", "e", "c", "a"}, found)
		}
	})
}

func TestScanAllAfterWithRotations(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.ScanAll("a", "b", 2, func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, []string{"c", "b"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c", "b"}, found)
		}
	})
}

func TestScanAllWithStop(t *testing.T) {
	withNode(func(n *Node) {
		n.SetRotateThreshold(1)

		trackevent(n, []byte("a"), map[string]string{"a": "b"})
		trackevent(n, []byte("b"), map[string]string{"a": "b"})
		trackevent(n, []byte("c"), map[string]string{"a": "b"})

		found := make([]string, 0)

		n.db.ScanAll("a", "b", 2, func(e *stream.Event) bool {
			found = append(found, string(e.Data))
			return false
		})

		if !reflect.DeepEqual(found, []string{"c"}) {
			t.Errorf("Incorrect stream results. Wanted: %v, found: %v", []string{"c"}, found)
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
