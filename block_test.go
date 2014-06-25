package esdb

import (
	"bytes"
	"reflect"
	"testing"
)

func create(id []byte) *Block {
	buffer := bytes.NewBuffer([]byte{})
	writer := newBlock(buffer, []byte("a"))
	populateBlock(writer)
	writer.write()

	return openBlock(bytes.NewReader(buffer.Bytes()), []byte("a"), 0, uint64(buffer.Len()))
}

func populateBlock(block *blockWriter) {
	es := events{
		newEvent(2, []byte("1")),
		newEvent(3, []byte("2")),
		newEvent(1, []byte("3")),
	}

	block.add(es[0].Data, es[0].Timestamp, "a", map[string]string{"ts": "", "i": "i1"})
	block.add(es[1].Data, es[1].Timestamp, "b", map[string]string{"ts": "", "i": "i2"})
	block.add(es[2].Data, es[2].Timestamp, "b", map[string]string{"ts": "", "i": "i1"})
}

func fetchPrimary(block *Block, primary string) []string {
	found := make([]string, 0)

	fetch := block.Scan

	fetch(primary, func(event *Event) bool {
		found = append(found, string(event.Data))
		return true
	})

	return found
}

func fetchIndex(block *Block, index, value string, reverse bool) []string {
	found := make([]string, 0)

	fetch := block.ScanIndex

	if reverse {
		fetch = block.RevScanIndex
	}

	fetch(index, value, func(event *Event) bool {
		found = append(found, string(event.Data))
		return true
	})

	return found
}

func TestBlockIndexScanning(t *testing.T) {
	block := create([]byte("a"))

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
		found := fetchIndex(block, test.index, test.value, test.reverse)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}

func TestBlockPrimaryScanning(t *testing.T) {
	block := create([]byte("a"))

	var tests = []struct {
		primary string
		want    []string
	}{
		{"a", []string{"1"}},
		{"b", []string{"2", "3"}},
	}

	for i, test := range tests {
		found := fetchPrimary(block, test.primary)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}
