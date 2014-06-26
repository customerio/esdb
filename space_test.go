package esdb

import (
	"bytes"
	"reflect"
	"testing"
)

func create(id []byte) *Space {
	buffer := bytes.NewBuffer([]byte{})
	writer := newSpace(buffer, []byte("a"))
	populateSpace(writer)
	writer.write()

	return openSpace(bytes.NewReader(buffer.Bytes()), []byte("a"), 0, uint64(buffer.Len()))
}

func populateSpace(space *spaceWriter) {
	es := events{
		newEvent(2, []byte("1")),
		newEvent(3, []byte("2")),
		newEvent(1, []byte("3")),
	}

	space.add(es[0].Data, es[0].Timestamp, "a", map[string]string{"ts": "", "i": "i1"})
	space.add(es[1].Data, es[1].Timestamp, "b", map[string]string{"ts": "", "i": "i2"})
	space.add(es[2].Data, es[2].Timestamp, "b", map[string]string{"ts": "", "i": "i1"})
}

func fetchPrimary(space *Space, primary string) []string {
	found := make([]string, 0)

	fetch := space.Scan

	fetch(primary, func(event *Event) bool {
		found = append(found, string(event.Data))
		return true
	})

	return found
}

func fetchIndex(space *Space, index, value string, reverse bool) []string {
	found := make([]string, 0)

	fetch := space.ScanIndex

	if reverse {
		fetch = space.RevScanIndex
	}

	fetch(index, value, func(event *Event) bool {
		found = append(found, string(event.Data))
		return true
	})

	return found
}

func TestSpace(t *testing.T) {
	space := create([]byte("a"))

	var tests = []struct {
		index   string
		value   string
		want    []string
		reverse bool
	}{
		{"ts", "", []string{"3", "1", "2"}, false},
		{"i", "i1", []string{"3", "1"}, false},
		{"i", "i2", []string{"2"}, false},
		{"i", "i3", []string{}, false},
		{"ts", "", []string{"2", "1", "3"}, true},
		{"i", "i1", []string{"1", "3"}, true},
		{"i", "i2", []string{"2"}, true},
	}

	for i, test := range tests {
		found := fetchIndex(space, test.index, test.value, test.reverse)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}

func TestSpacePrimaryScanning(t *testing.T) {
	space := create([]byte("a"))

	var tests = []struct {
		primary string
		want    []string
	}{
		{"a", []string{"1"}},
		{"b", []string{"2", "3"}},
	}

	for i, test := range tests {
		found := fetchPrimary(space, test.primary)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}
