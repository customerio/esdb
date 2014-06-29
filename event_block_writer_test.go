package esdb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/customerio/esdb/blocks"
)

func generate(length int) []byte {
	b := make([]byte, length)

	for i := 0; i < length; i++ {
		b[i] = byte('a')
	}

	return b
}

func TestWriteEventBlocksSmall(t *testing.T) {
	w := new(bytes.Buffer)

	e1 := newEvent([]byte("abc"), 1)
	e2 := newEvent([]byte("b"), 3)
	e3 := newEvent([]byte("c"), 2)
	e4 := newEvent([]byte("def"), 4)

	index := &index{evs: events{e1, e2, e3, e4}}

	writeEventBlocks(index, w)

	expected := []byte("\x0d\x00\x00\x03def\x01b\x01c\x03abc\x00")

	if !reflect.DeepEqual(w.Bytes(), expected) {
		t.Errorf("Wrong event block bytecode:\n wanted: %x\n found:  %x", expected, w.Bytes())
	}

	want := events{e4, e2, e3, e1}

	if !reflect.DeepEqual(index.evs, want) {
		t.Errorf("Wrongly sorted events: wanted: %v found: %v", want, index.evs)
	}

	var tests = []struct {
		event  *Event
		block  int64
		offset int
	}{
		{e1, 0, 8},
		{e2, 0, 4},
		{e3, 0, 6},
		{e4, 0, 0},
	}

	for i, test := range tests {
		event := test.event

		if event.block != test.block || event.offset != test.offset {
			t.Errorf("Case %d: Wrong event block/offset. wanted: %d,%d found: %d,%d", i, test.block, test.offset, event.block, event.offset)
		}
	}
}

func TestWriteEventBlocksMedium(t *testing.T) {
	w := new(bytes.Buffer)

	e1 := newEvent(generate(2600), 1)
	e2 := newEvent(generate(2800), 3)
	e3 := newEvent(generate(2200), 2)
	e4 := newEvent(generate(3400), 4)

	index := &index{evs: events{e1, e2, e3, e4}}

	writeEventBlocks(index, w)

	var tests = []struct {
		event  *Event
		block  int64
		offset int
	}{
		{e1, 411, 214},
		{e2, 0, 3402},
		{e3, 206, 2108},
		{e4, 0, 0},
	}

	for i, test := range tests {
		event := test.event

		if event.block != test.block || event.offset != test.offset {
			t.Errorf("Case %d: Wrong event block/offset. wanted: %d,%d found: %d,%d", i, test.block, test.offset, event.block, event.offset)
		}
	}

	r := blocks.NewByteReader(w.Bytes(), 4096)

	for i, dataLen := range []int{3400, 2800, 2200, 2600} {
		e := pullEvent(r)

		if len(e.Data) != dataLen {
			t.Errorf("Case %d: Wrong read data. wanted: %d bytes found: %d bytes", i, dataLen, len(e.Data))
		}
	}

	if e := pullEvent(r); e != nil {
		t.Errorf("Case %d: Found unexpected written event %v", e.Data)
	}
}

func TestWriteEventBlocksLarge(t *testing.T) {
	w := new(bytes.Buffer)

	e1 := newEvent(generate(20000), 1)
	e2 := newEvent(generate(20000), 3)
	e3 := newEvent(generate(20000), 2)
	e4 := newEvent(generate(20000), 4)

	index := &index{evs: events{e1, e2, e3, e4}}

	writeEventBlocks(index, w)

	var tests = []struct {
		event  *Event
		block  int64
		offset int
	}{
		{e1, 2802, 2665},
		{e2, 799, 3619},
		{e3, 1801, 3142},
		{e4, 0, 0},
	}

	for i, test := range tests {
		event := test.event

		if event.block != test.block || event.offset != test.offset {
			t.Errorf("Case %d: Wrong event block/offset. wanted: %d,%d found: %d,%d", i, test.block, test.offset, event.block, event.offset)
		}
	}

	r := blocks.NewByteReader(w.Bytes(), 4096)

	for i, dataLen := range []int{20000, 20000, 20000, 20000} {
		e := pullEvent(r)

		if len(e.Data) != dataLen {
			t.Errorf("Case %d: Wrong read data. wanted: %d bytes found: %d bytes", i, dataLen, len(e.Data))
		}
	}

	if e := pullEvent(r); e != nil {
		t.Errorf("Case %d: Found unexpected written event %v", e.Data)
	}
}
