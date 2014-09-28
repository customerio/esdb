package stream

import (
	"log"
	"os"
	"reflect"
	"testing"
)

func createStream() Stream {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.stream")

	return newStream()
}

func newStream() Stream {
	s, err := New("tmp/test.stream")
	if err != nil {
		log.Fatalf(err.Error())
	}

	return s
}

func reopenStream() Stream {
	s, err := Read("tmp/test.stream")
	if err != nil {
		log.Fatalf(err.Error())
	}

	return s
}

func TestTails(t *testing.T) {
	s := createStream()

	len1, _ := s.Write([]byte("abc"), []string{"a", "b", "c"})
	len2, _ := s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	var tests = []struct {
		index  string
		offset int64
	}{
		{"a", int64(len(MAGIC))},
		{"b", int64(len(MAGIC))},
		{"c", int64(len(MAGIC) + len1)},
		{"d", int64(len(MAGIC) + len1 + len2)},
		{"e", int64(len(MAGIC) + len1 + len2)},
		{"f", int64(len(MAGIC) + len1 + len2)},
	}

	for i, test := range tests {
		if off := s.(*openStream).tails[test.index]; off != test.offset {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.offset, off)
		}
	}
}

func TestScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	var tests = []struct {
		index  string
		events []string
	}{
		{"a", []string{"abc"}},
		{"b", []string{"abc"}},
		{"c", []string{"cde", "abc"}},
		{"d", []string{"def", "cde"}},
		{"e", []string{"def", "cde"}},
		{"f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}
}

func TestIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	found := make([]string, 0)

	s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestReopenScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	s = reopenStream()

	var tests = []struct {
		index  string
		events []string
	}{
		{"a", []string{"abc"}},
		{"b", []string{"abc"}},
		{"c", []string{"cde", "abc"}},
		{"d", []string{"def", "cde"}},
		{"e", []string{"def", "cde"}},
		{"f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}
}

func TestReopenIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	s = reopenStream()

	found := make([]string, 0)

	s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

// TODO failure of half-written event
// TODO recovery of half-written event
// TODO scan and iterate on reopened stream
// TODO test closing stream
// TODO test error when writing to closed stream
