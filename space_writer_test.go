package esdb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/customerio/esdb/binary"
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

	e1data := []byte("abc")
	e2data := []byte("b")
	e3data := []byte("c")
	e4data := []byte("def")
	e1 := newEvent(e1data, 1)
	e2 := newEvent(e2data, 3)
	e3 := newEvent(e3data, 2)
	e4 := newEvent(e4data, 4)

	writer.add(e1, "1", nil)
	writer.add(e2, "2", nil)
	writer.add(e3, "2", nil)
	writer.add(e4, "1", nil)

	writer.write()

	sst, _ := findSpaceIndex(bytes.NewReader(w.Bytes()), 0, int64(w.Len()))

	var tests = []struct {
		key        string
		offset     int
		length     int
		data       [][]byte
		timestamps []int
	}{
		{"g1", 1, 20, [][]byte{e4data, e1data}, []int{4, 1}},
		{"g2", 21, 16, [][]byte{e2data, e3data}, []int{3, 2}},
	}

	for i, test := range tests {
		val, _ := sst.Get([]byte(test.key))
		r := bytes.NewReader(val)

		offset := binary.ReadUvarint(r)
		length := binary.ReadUvarint(r)

		if int(offset) != test.offset || int(length) != test.length {
			t.Errorf("Case %d: Wrong grouping encoding: want: %d,%d found: %d,%d", i, test.offset, test.length, offset, length)
		}

		reader := blocks.NewByteReader(w.Bytes(), 4096)
		reader.Seek(int64(offset), 0)

		for j, data := range test.data {
			found := pullEvent(reader)

			if string(found.Data) != string(data) {
				t.Errorf("Case %d/%d: Wrong event data found: want: %s found: %s", i, j, data, found.Data)
			}
		}

		reader.Seek(int64(offset), 0)

		for j, ts := range test.timestamps {
			found := pullEvent(reader)

			if found.Timestamp != ts {
				t.Errorf("Case %d/%d: Wrong event timestamp found: want: %d found: %d", i, j, ts, found.Timestamp)
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

	e1data := []byte("abc")
	e2data := []byte("b")
	e3data := []byte("c")
	e4data := []byte("def")
	e1 := newEvent(e1data, 1)
	e2 := newEvent(e2data, 3)
	e3 := newEvent(e3data, 2)
	e4 := newEvent(e4data, 4)

	writer.add(e1, "1", map[string]string{"a": "1"})
	writer.add(e2, "1", map[string]string{"a": "2"})
	writer.add(e3, "1", map[string]string{"a": "2"})
	writer.add(e4, "1", map[string]string{"a": "1"})

	writer.write()

	var tests = []struct {
		key     string
		offset  int
		length  int
		evs     [][]byte
		indexed events
	}{
		{"g1", 1, 32, [][]byte{e4data, e2data, e3data, e1data}, nil},
		{"ia:1", 33, 21, nil, events{e4, e1}},
		{"ia:2", 54, 23, nil, events{e2, e3}},
	}

	sst, _ := findSpaceIndex(bytes.NewReader(w.Bytes()), 0, int64(w.Len()))

	for i, test := range tests {
		val, _ := sst.Get([]byte(test.key))

		r := bytes.NewReader(val)

		offset := binary.ReadUvarint(r)
		length := binary.ReadUvarint(r)

		if int(offset) != test.offset || int(length) != test.length {
			t.Errorf("Case %d: Wrong grouping encoding: want: %d,%d found: %d,%d", i, test.offset, test.length, offset, length)
		}

		reader := blocks.NewByteReader(w.Bytes(), 4096)
		reader.Seek(int64(offset), 0)

		if len(test.evs) > 0 {
			for j, data := range test.evs {
				if e := pullEvent(reader); !reflect.DeepEqual(e.Data, data) {
					t.Errorf("Case %d/%d: Wrong event found: want: %s found: %s", i, j, data, e.Data)
				}
			}

			if e := pullEvent(reader); e != nil {
				t.Errorf("Wrong event found: want: nil found: %s", e.Data)
			}
		}

		for j, event := range test.indexed {
			block := binary.ReadInt64(reader)
			offset := binary.ReadInt16(reader)

			if block != event.block || int(offset) != event.offset {
				t.Errorf("Case %d/%d: Wrong event index: want: %d,%d found: %d,%d", i, j, event.block, event.offset, block, offset)
			}
		}
	}
}
