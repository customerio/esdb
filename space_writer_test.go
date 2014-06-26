package esdb

import (
	"bytes"
	"reflect"
	"testing"
)

func TestWriteSpaceImmutability(t *testing.T) {
	writer := newSpace(new(bytes.Buffer), []byte("a"))
	writer.write()

	err := writer.add(newEvent([]byte("1"), 1), "b", nil)

	if err == nil || err.Error() != "Cannot add to space. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written space")
	}
}

func TestWriteSpaceGrouping(t *testing.T) {
	w := new(bytes.Buffer)

	writer := newSpace(w, []byte("a"))

	e1 := newEvent([]byte("abc"), 1)
	e2 := newEvent([]byte("b"), 3)
	e3 := newEvent([]byte("c"), 2)
	e4 := newEvent([]byte("def"), 4)

	writer.add(e1, "1", nil)
	writer.add(e2, "2", nil)
	writer.add(e3, "2", nil)
	writer.add(e4, "1", nil)

	writer.write()

	sst, _ := findSpaceIndex(bytes.NewReader(w.Bytes()), 0, uint64(w.Len()))

	var tests = []struct {
		key    string
		offset int
		length int
		evs    events
	}{
		{"g1", 1, 9, events{e4, e1}},
		{"g2", 10, 5, events{e2, e3}},
	}

	for i, test := range tests {
		val, _ := sst.Get([]byte(test.key))
		buf := newBuffer(bytes.NewReader(val), 0, uint64(len(val)), 0)

		offset := buf.PullUvarint()
		length := buf.PullUvarint()

		if offset != test.offset || length != test.length {
			t.Errorf("Case %d: Wrong grouping encoding: want: %d,%d found: %d,%d", i, test.offset, test.length, offset, length)
		}

		buf = newBuffer(bytes.NewReader(w.Bytes()), uint64(offset), uint64(offset+length), 0)

		for j, event := range test.evs {
			if e := pullEvent(buf); !reflect.DeepEqual(e.Data, event.Data) {
				t.Errorf("Case %d/%d: Wrong event found: want: %s found: %s", i, j, event.Data, e.Data)
			}
		}

		if e := pullEvent(buf); e != nil {
			t.Errorf("Wrong event found: want: nil found: %s", e.Data)
		}
	}
}

func TestWriteSpaceIndexes(t *testing.T) {
	w := new(bytes.Buffer)

	writer := newSpace(w, []byte("a"))

	e1 := newEvent([]byte("abc"), 1)
	e2 := newEvent([]byte("b"), 3)
	e3 := newEvent([]byte("c"), 2)
	e4 := newEvent([]byte("def"), 4)

	writer.add(e1, "1", map[string]string{"a": "1"})
	writer.add(e2, "1", map[string]string{"a": "2"})
	writer.add(e3, "1", map[string]string{"a": "2"})
	writer.add(e4, "1", map[string]string{"a": "1"})

	writer.write()

	var tests = []struct {
		key     string
		offset  int
		length  int
		evs     events
		indexed events
	}{
		{"g1", 1, 13, events{e4, e2, e3, e1}, nil},
		{"ia:1", 14, 20, nil, events{e4, e1}},
		{"ia:2", 34, 20, nil, events{e2, e3}},
	}

	sst, _ := findSpaceIndex(bytes.NewReader(w.Bytes()), 0, uint64(w.Len()))

	for i, test := range tests {
		val, _ := sst.Get([]byte(test.key))

		buf := newBuffer(bytes.NewReader(val), 0, uint64(len(val)), 0)

		offset := buf.PullUvarint()
		length := buf.PullUvarint()

		buf = newBuffer(bytes.NewReader(w.Bytes()), uint64(offset), uint64(offset+length), 0)

		if offset != test.offset || length != test.length {
			t.Errorf("Case %d: Wrong grouping encoding: want: %d,%d found: %d,%d", i, test.offset, test.length, offset, length)
		}

		if len(test.evs) > 0 {
			for j, event := range test.evs {
				if e := pullEvent(buf); !reflect.DeepEqual(e.Data, event.Data) {
					t.Errorf("Case %d/%d: Wrong event found: want: %s found: %s", i, j, event.Data, e.Data)
				}
			}

			if e := pullEvent(buf); e != nil {
				t.Errorf("Wrong event found: want: nil found: %s", e.Data)
			}
		}

		for j, event := range test.indexed {
			block := buf.PullUint64()
			offset := buf.PullUint16()

			if int(block) != event.block || int(offset) != event.offset {
				t.Errorf("Case %d/%d: Wrong event index: want: %d,%d found: %d,%d", i, j, event.block, event.offset, block, offset)
			}
		}
	}
}
