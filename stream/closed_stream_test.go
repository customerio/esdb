package stream

import (
	"os"
	"reflect"
	"testing"
)

func buildStream() Stream {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.stream")

	s := newStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})
	s.Close()

	return reopenStream()
}

func TestClosed(t *testing.T) {
	s := buildStream()

	if !s.Closed() {
		t.Errorf("Closed stream is not closed.")
	}
}

func TestClosedScan(t *testing.T) {
	s := buildStream()

	var tests = []struct {
		index  string
		value  string
		events []string
	}{
		{"a", "a", []string{"abc"}},
		{"b", "b", []string{"abc"}},
		{"c", "c", []string{"cde", "abc"}},
		{"d", "d", []string{"def", "cde"}},
		{"e", "e", []string{"def", "cde"}},
		{"f", "f", []string{"def"}},
		{"g", "g", []string{}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		err := s.ScanIndex(test.index, test.value, 0, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if err != nil {
			t.Errorf("Case #%v: found err: %v", i, err)
		}

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}
}

func TestClosedContinueScan(t *testing.T) {
	s := buildStream()

	var offset int64
	found := make([]string, 0)

	s.ScanIndex("c", "c", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("c", "c")
		return false
	})
	s.ScanIndex("c", "c", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("c", "c")
		return false
	})

	if offset != 0 {
		t.Errorf("Wanted offset: 0, found: %v", offset)
	}

	if !reflect.DeepEqual(found, []string{"cde", "abc"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"cde", "abc"}, found)
	}
}

func TestClosedIterate(t *testing.T) {
	s := buildStream()

	found := make([]string, 0)

	_, err := s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if err != nil {
		t.Errorf("Error found while iterating: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}

	found = make([]string, 0)

	_, err = s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return len(found) != 2
	})

	if err != nil {
		t.Errorf("Error found while iterating: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"abc", "cde"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde"}, found)
	}
}

func TestClosedContinueIterate(t *testing.T) {
	s := buildStream()

	var offset int64
	found := make([]string, 0)

	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})
	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})
	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})
	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestClosedWrite(t *testing.T) {
	s := buildStream()

	_, err := s.Write([]byte("efg"), map[string]string{"e": "e", "f": "f", "g": "g"})
	if err == nil {
		t.Errorf("No error found while writing to closed stream.")
	}
}
