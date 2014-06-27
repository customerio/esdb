package esdb

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"

	"github.com/customerio/esdb/blocks"
)

func TestWriteIndexBlocksSmall(t *testing.T) {
	w := new(bytes.Buffer)

	e1 := &Event{Timestamp: 1, block: 0, offset: 2048}
	e2 := &Event{Timestamp: 3, block: 512, offset: 128}
	e3 := &Event{Timestamp: 2, block: 2048, offset: 512}
	e4 := &Event{Timestamp: 4, block: 248, offset: 1024}

	index := &index{evs: events{e1, e2, e3, e4}}

	writeIndexBlocks(index, w)

	if index.length != 44 {
		t.Errorf("Wrong written length: wanted: 44, found: %d", index.length)
	}

	expected := []byte(
		"\x2a\x00" +
			"\x00" +
			"\x00\x00\x00\x00\x00\x00\x00\x00" + "\x00\x08" +
			"\x00\x08\x00\x00\x00\x00\x00\x00" + "\x00\x02" +
			"\x00\x02\x00\x00\x00\x00\x00\x00" + "\x80\x00" +
			"\xF8\x00\x00\x00\x00\x00\x00\x00" + "\x00\x04" +
			"\x00",
	)

	if !reflect.DeepEqual(w.Bytes(), expected) {
		t.Errorf("Wrong event block bytecode:\n wanted: %x\n found:  %x", expected, w.Bytes())
	}

	want := events{e1, e3, e2, e4}

	if !reflect.DeepEqual(index.evs, want) {
		t.Errorf("Wrongly sorted events: wanted: %v found: %v", want, index.evs)
	}

	buf := newBuffer(blocks.NewByteReader(w.Bytes(), 4096), 0, uint64(len(w.Bytes())), len(w.Bytes()))

	buf.Pull(1)

	for i, event := range index.evs {
		block := int(buf.PullUint64())
		offset := int(buf.PullUint16())

		if event.block != block || event.offset != offset {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}

func TestWriteIndexBlocksMedium(t *testing.T) {
	w := new(bytes.Buffer)

	index := &index{evs: make(events, 500)}

	for i := 0; i < 500; i++ {
		index.evs[i] = &Event{Timestamp: i, block: int(rand.Int63()), offset: rand.Intn(4096)}
	}

	writeIndexBlocks(index, w)

	if index.length != 5006 {
		t.Errorf("Wrong written length: wanted: 5006, found: %d", index.length)
	}

	buf := newBuffer(blocks.NewByteReader(w.Bytes(), 4096), 0, uint64(len(w.Bytes())), len(w.Bytes()))

	buf.Pull(1)

	for i, event := range index.evs {
		block := int(buf.PullUint64())
		offset := int(buf.PullUint16())

		if event.block != block || event.offset != offset {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}

func TestWriteIndexBlocksLarge(t *testing.T) {
	w := new(bytes.Buffer)

	index := &index{evs: make(events, 5000)}

	for i := 0; i < 5000; i++ {
		index.evs[i] = &Event{Timestamp: i, block: int(rand.Int63()), offset: rand.Intn(4096)}
	}

	writeIndexBlocks(index, w)

	if index.length != 50028 {
		t.Errorf("Wrong written length: wanted: 50028, found: %d", index.length)
	}

	buf := newBuffer(blocks.NewByteReader(w.Bytes(), 4096), 0, uint64(len(w.Bytes())), len(w.Bytes()))

	buf.Pull(1)

	for i, event := range index.evs {
		block := int(buf.PullUint64())
		offset := int(buf.PullUint16())

		if event.block != block || event.offset != offset {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}
