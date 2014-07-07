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

	expected := []byte("\x1d\x00\x00\x03\x04\x00\x00\x00def\x01\x03\x00\x00\x00b\x01\x02\x00\x00\x00c\x03\x01\x00\x00\x00abc\x00")

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
		{e1, 0, 20},
		{e2, 0, 8},
		{e3, 0, 14},
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
		{e1, 419, 226},
		{e2, 0, 3406},
		{e3, 211, 2116},
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
		{e1, 2815, 2677},
		{e2, 803, 3623},
		{e3, 1809, 3150},
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
