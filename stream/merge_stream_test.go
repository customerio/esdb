package stream

import (
	"log"
	"os"
	"reflect"
	"testing"
)

func createStreamNamed(name string) Stream {
	path := "tmp/" + name

	os.MkdirAll("tmp", 0755)
	os.Remove(path)

	s, err := New(path)
	if err != nil {
		log.Fatalf(err.Error())
	}

	s.Write([]byte("abc"+name), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"+name), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"+name), map[string]string{"d": "d", "e": "e", "f": "f"})
	s.Close()

	s, err = Open(path)
	if err != nil {
		log.Fatalf(err.Error())
	}

	return s
}

func TestMergeIterate(t *testing.T) {
	createStreamNamed("one")
	createStreamNamed("two")
	createStreamNamed("three")

	os.Remove("tmp/merged.stream")
	err := Merge("tmp/merged.stream", []string{"tmp/one", "tmp/two", "tmp/three"})

	if err != nil {
		t.Errorf("found err: %v", err)
	}

	m, _ := Open("tmp/merged.stream")

	found := make([]string, 0)

	_, err = m.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if err != nil {
		t.Errorf("found err: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"abcone", "cdeone", "defone", "abctwo", "cdetwo", "deftwo", "abcthree", "cdethree", "defthree"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abcone", "cdeone", "defone", "abctwo", "cdetwo", "deftwo", "abcthree", "cdethree", "defthree"}, found)
	}
}

func TestMergeScanIndex(t *testing.T) {
	createStreamNamed("one")
	createStreamNamed("two")
	createStreamNamed("three")

	os.Remove("tmp/merged.stream")
	err := Merge("tmp/merged.stream", []string{"tmp/one", "tmp/two", "tmp/three"})

	if err != nil {
		t.Errorf("found err: %v", err)
	}

	m, _ := Open("tmp/merged.stream")

	found := make([]string, 0)

	err = m.ScanIndex("d", "d", 0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if err != nil {
		t.Errorf("found err: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"defthree", "cdethree", "deftwo", "cdetwo", "defone", "cdeone"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"defthree", "cdethree", "deftwo", "cdetwo", "defone", "cdeone"}, found)
	}
}
