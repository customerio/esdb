package esdb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/customerio/esdb/blocks"
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

	sst, _ := findSpaceIndex(bytes.NewReader(w.Bytes()), 0, int64(w.Len()))

	var tests = []struct {
		key    string
		offset int
		length int
		evs    events
	}{
		{"g1", 1, 12, events{e4, e1}},
		{"g2", 13, 8, events{e2, e3}},
	}

	for i, test := range tests {
		val, _ := sst.Get([]byte(test.key))
		r := bytes.NewReader(val)

		offset := readUvarint(r)
		length := readUvarint(r)

		if int(offset) != test.offset || int(length) != test.length {
			t.Errorf("Case %d: Wrong grouping encoding: want: %d,%d found: %d,%d", i, test.offset, test.length, offset, length)
		}

		reader := blocks.NewByteReader(w.Bytes(), 4096)
		reader.Seek(int64(offset), 0)

		for j, event := range test.evs {
			if e := pullEvent(reader); !reflect.DeepEqual(e.Data, event.Data) {
				t.Errorf("Case %d/%d: Wrong event found: want: %s found: %s", i, j, event.Data, e.Data)
			}
		}

		if e := pullEvent(reader); e != nil {
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
		{"g1", 1, 16, events{e4, e2, e3, e1}, nil},
		{"ia:1", 17, 21, nil, events{e4, e1}},
		{"ia:2", 38, 23, nil, events{e2, e3}},
	}

	sst, _ := findSpaceIndex(bytes.NewReader(w.Bytes()), 0, int64(w.Len()))

	for i, test := range tests {
		val, _ := sst.Get([]byte(test.key))

		r := bytes.NewReader(val)

		offset := readUvarint(r)
		length := readUvarint(r)

		if int(offset) != test.offset || int(length) != test.length {
			t.Errorf("Case %d: Wrong grouping encoding: want: %d,%d found: %d,%d", i, test.offset, test.length, offset, length)
		}

		reader := blocks.NewByteReader(w.Bytes(), 4096)
		reader.Seek(int64(offset), 0)

		if len(test.evs) > 0 {
			for j, event := range test.evs {
				if e := pullEvent(reader); !reflect.DeepEqual(e.Data, event.Data) {
					t.Errorf("Case %d/%d: Wrong event found: want: %s found: %s", i, j, event.Data, e.Data)
				}
			}

			if e := pullEvent(reader); e != nil {
				t.Errorf("Wrong event found: want: nil found: %s", e.Data)
			}
		}

		for j, event := range test.indexed {
			block := readInt64(reader)
			offset := readInt16(reader)

			if int(block) != event.block || int(offset) != event.offset {
				t.Errorf("Case %d/%d: Wrong event index: want: %d,%d found: %d,%d", i, j, event.block, event.offset, block, offset)
			}
		}
	}
}
