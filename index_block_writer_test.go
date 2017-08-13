package esdb

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/blocks"
	"github.com/golang/snappy"
)

func TestWriteIndexBlocksSmall(t *testing.T) {
	w := new(bytes.Buffer)

	e1 := &Event{Timestamp: 1, block: 0, offset: 2048}
	e2 := &Event{Timestamp: 3, block: 512, offset: 128}
	e3 := &Event{Timestamp: 2, block: 2048, offset: 512}
	e4 := &Event{Timestamp: 4, block: 248, offset: 1024}

	index := &index{evs: events{e1, e2, e3, e4}}

	writeIndexBlocks(index, w)

	if index.length != 36 {
		t.Errorf("Wrong written length: wanted: 36, found: %d", index.length)
	}

	compressed := snappy.Encode(nil, []byte(
		"\xF8\x00\x00\x00\x00\x00\x00\x00"+"\x00\x04"+
			"\x00\x02\x00\x00\x00\x00\x00\x00"+"\x80\x00"+
			"\x00\x08\x00\x00\x00\x00\x00\x00"+"\x00\x02"+
			"\x00\x00\x00\x00\x00\x00\x00\x00"+"\x00\x08"+
			"\x00"))

	expected := append([]byte("\x21\x00\x01"), compressed...)

	if !reflect.DeepEqual(w.Bytes(), expected) {
		t.Errorf("Wrong event block bytecode:\n wanted: %x\n found:  %x", expected, w.Bytes())
	}

	want := events{e4, e2, e3, e1}

	if !reflect.DeepEqual(index.evs, want) {
		t.Errorf("Wrongly sorted events: wanted: %v found: %v", want, index.evs)
	}

	reader := blocks.NewByteReader(w.Bytes(), 4096)

	for i, event := range index.evs {
		block := binary.ReadInt64(reader)
		offset := binary.ReadInt16(reader)

		if event.block != block || event.offset != int(offset) {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}

func TestWriteIndexBlocksMedium(t *testing.T) {
	w := new(bytes.Buffer)

	index := &index{evs: make(events, 500)}

	for i := 0; i < 500; i++ {
		index.evs[i] = &Event{Timestamp: i, block: rand.Int63(), offset: rand.Intn(4096)}
	}

	writeIndexBlocks(index, w)

	if index.length != 5007 {
		t.Errorf("Wrong written length: wanted: 5007, found: %d", index.length)
	}

	reader := blocks.NewByteReader(w.Bytes(), 4096)

	for i, event := range index.evs {
		block := binary.ReadInt64(reader)
		offset := binary.ReadInt16(reader)

		if event.block != block || event.offset != int(offset) {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}

func TestWriteIndexBlocksLarge(t *testing.T) {
	w := new(bytes.Buffer)

	index := &index{evs: make(events, 5000)}

	for i := 0; i < 5000; i++ {
		index.evs[i] = &Event{Timestamp: i, block: rand.Int63(), offset: rand.Intn(4096)}
	}

	writeIndexBlocks(index, w)

	if index.length != 50040 {
		t.Errorf("Wrong written length: wanted: 50040, found: %d", index.length)
	}

	reader := blocks.NewByteReader(w.Bytes(), 4096)

	for i, event := range index.evs {
		block := binary.ReadInt64(reader)
		offset := binary.ReadInt16(reader)

		if event.block != block || event.offset != int(offset) {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}
