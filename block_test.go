package esdb

import (
	"reflect"
	"testing"
)

func populateBlock(block *Block) {
	events = Events{
		&Event{Timestamp: 2, Data: []byte("1")},
		&Event{Timestamp: 3, Data: []byte("2")},
		&Event{Timestamp: 1, Data: []byte("3")},
	}

	block.add(events[0].Timestamp, events[0].Data, []string{"", "i1", "i2"})
	block.add(events[1].Timestamp, events[1].Data, []string{"", "i2"})
	block.add(events[2].Timestamp, events[2].Data, []string{"", "i1"})
}

func fetchEvents(block *Block, index string, reverse bool) []*Event {
	found := make([]*Event, 0)

	fetch := block.Scan

	if reverse {
		fetch = block.RevScan
	}

	fetch(index, func(event *Event) bool {
		found = append(found, event)
		return true
	})

	return found
}

func TestBlockImmutability(t *testing.T) {
	block := newBlock(nil, []byte("a"), 0, 100)

	err := block.add(1, []byte("1"), []string{"", "i2"})

	if err == nil || err.Error() != "Cannot add to block. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written block")
	}
}

func TestBlockScanning(t *testing.T) {
	block := createBlock(nil, []byte("a"), "")
	populateBlock(block)

	var tests = []struct {
		index   string
		want    []*Event
		reverse bool
	}{
		{"", []*Event{events[2], events[0], events[1]}, false},
		{"i1", []*Event{events[2], events[0]}, false},
		{"i2", []*Event{events[0], events[1]}, false},
		{"i3", []*Event{}, false},
		{"", []*Event{events[1], events[0], events[2]}, true},
		{"i1", []*Event{events[0], events[2]}, true},
		{"i2", []*Event{events[1], events[0]}, true},
	}

	for i, test := range tests {
		found := fetchEvents(block, test.index, test.reverse)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}
