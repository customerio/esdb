package esdb

import (
	"bytes"
	"reflect"
	"testing"
)

func create(id []byte) *Block {
	buffer := bytes.NewBuffer([]byte{})
	block := createBlock(buffer, []byte("a"))
	populateBlock(block)
	block.write()

	return newBlock(bytes.NewReader(buffer.Bytes()), []byte("a"), 0, int64(buffer.Len()))
}

func populateBlock(block *Block) {
	events = Events{
		newEvent(2, []byte("1")),
		newEvent(3, []byte("2")),
		newEvent(1, []byte("3")),
	}

	block.add(events[0].Timestamp, events[0].Data, "a", []string{"", "i1", "i2"})
	block.add(events[1].Timestamp, events[1].Data, "b", []string{"", "i2"})
	block.add(events[2].Timestamp, events[2].Data, "b", []string{"", "i1"})
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

func fetchIndex(block *Block, index string, reverse bool) []string {
	found := make([]string, 0)

	fetch := block.ScanIndex

	if reverse {
		fetch = block.RevScanIndex
	}

	fetch(index, func(event *Event) bool {
		found = append(found, string(event.Data))
		return true
	})

	return found
}

func TestBlockImmutability(t *testing.T) {
	block := newBlock(nil, []byte("a"), 0, 100)

	err := block.add(1, []byte("1"), "b", []string{"", "i2"})

	if err == nil || err.Error() != "Cannot add to block. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written block")
	}
}

func TestBlockIndexScanning(t *testing.T) {
	block := create([]byte("a"))

	var tests = []struct {
		index   string
		want    []string
		reverse bool
	}{
		{"", []string{"3", "1", "2"}, false},
		{"i1", []string{"3", "1"}, false},
		{"i2", []string{"1", "2"}, false},
		{"i3", []string{}, false},
		{"", []string{"2", "1", "3"}, true},
		{"i1", []string{"1", "3"}, true},
		{"i2", []string{"2", "1"}, true},
	}

	for i, test := range tests {
		found := fetchIndex(block, test.index, test.reverse)

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
