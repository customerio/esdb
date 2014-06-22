package esdb

import (
	"reflect"
	"testing"
)

func fetchId(db *Db, id []byte, index string) []*Event {
	found := make([]*Event, 0)

	block := db.Find(id)

	if block != nil {
		block.Scan(index, func(event *Event) bool {
			found = append(found, event)
			return true
		})
	}

	return found
}

var events Events

func populate(db *Db) {
	events = Events{
		&Event{Timestamp: 2, Data: []byte("1")},
		&Event{Timestamp: 3, Data: []byte("2")},
		&Event{Timestamp: 1, Data: []byte("3")},
		&Event{Timestamp: 1, Data: []byte("4")},
		&Event{Timestamp: 1, Data: []byte("5")},
		&Event{Timestamp: 2, Data: []byte("6")},
	}

	db.Add([]byte("a"), events[0].Timestamp, events[0].Data, "", []string{"i1", "i2"})
	db.Add([]byte("a"), events[1].Timestamp, events[1].Data, "", []string{"i2"})
	db.Add([]byte("a"), events[2].Timestamp, events[2].Data, "", []string{"i1"})
	db.Add([]byte("b"), events[3].Timestamp, events[3].Data, "i1", []string{""})
	db.Add([]byte("b"), events[4].Timestamp, events[4].Data, "i1", []string{""})
	db.Add([]byte("b"), events[5].Timestamp, events[5].Data, "i1", []string{"", "i2"})
}

func TestDbImmutability(t *testing.T) {
	db, _ := Open("tmp/test.esdb")

	err := db.Add([]byte("b"), 1, []byte("1"), "i1", []string{"", "i2"})

	if err == nil || err.Error() != "Cannot add to database. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written database")
	}
}

func TestDbAdd(t *testing.T) {
	db, _ := Create("tmp/test.esdb")
	populate(db)

	var tests = []struct {
		id    string
		index string
		want  []*Event
	}{
		{"a", "", []*Event{events[2], events[0], events[1]}},
		{"a", "i1", []*Event{events[2], events[0]}},
		{"a", "i2", []*Event{events[0], events[1]}},
		{"b", "", []*Event{events[3], events[4], events[5]}},
		{"b", "i1", []*Event{events[3], events[4], events[5]}},
		{"b", "i2", []*Event{events[5]}},
		{"b", "i3", []*Event{}},
		{"c", "", []*Event{}},
	}

	for i, test := range tests {
		found := fetchId(db, []byte(test.id), test.index)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}
