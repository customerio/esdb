package esdb

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func TestWriteIndexBlocksSmall(t *testing.T) {
	w := new(bytes.Buffer)

	e1 := &Event{Timestamp: 1, block: 0, offset: 2048}
	e2 := &Event{Timestamp: 3, block: 512, offset: 128}
	e3 := &Event{Timestamp: 2, block: 2048, offset: 512}
	e4 := &Event{Timestamp: 4, block: 248, offset: 1024}

	index := &index{evs: events{e1, e2, e3, e4}}

	blocks := writeIndexBlocks(index, w)

	if !reflect.DeepEqual(blocks, []int{0}) {
		t.Errorf("Wrong number of event blocks: wanted: [0], found: %#v", blocks)
	}

	if index.length != 40 {
		t.Errorf("Wrong written length: wanted: 40, found: %d", index.length)
	}

	expected := []byte(
		"\x00\x00\x00\x00\x00\x00\x00\x00" + "\x00\x08" +
			"\x00\x08\x00\x00\x00\x00\x00\x00" + "\x00\x02" +
			"\x00\x02\x00\x00\x00\x00\x00\x00" + "\x80\x00" +
			"\xF8\x00\x00\x00\x00\x00\x00\x00" + "\x00\x04",
	)

	if !reflect.DeepEqual(w.Bytes(), expected) {
		t.Errorf("Wrong event block bytecode:\n wanted: %x\n found:  %x", expected, w.Bytes())
	}

	want := events{e1, e3, e2, e4}

	if !reflect.DeepEqual(index.evs, want) {
		t.Errorf("Wrongly sorted events: wanted: %v found: %v", want, index.evs)
	}

	buf := newByteBuffer(w.Bytes())

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

	blocks := writeIndexBlocks(index, w)

	if !reflect.DeepEqual(blocks, []int{0, 4096}) {
		t.Errorf("Wrong number of event blocks: wanted: [0, 4096], found: %#v", blocks)
	}

	if index.length != 5000 {
		t.Errorf("Wrong written length: wanted: 5000, found: %d", index.length)
	}

	buf := newByteBuffer(w.Bytes())

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

	blocks := writeIndexBlocks(index, w)

	if !reflect.DeepEqual(blocks, []int{0, 4096, 8192, 12288, 16384, 20480, 24576, 28672, 32768, 36864, 40960, 45056, 49152}) {
		t.Errorf("Wrong number of event blocks: wanted: 12*4096?, found: %#v", blocks)
	}

	if index.length != 50000 {
		t.Errorf("Wrong written length: wanted: 50000, found: %d", index.length)
	}

	buf := newByteBuffer(w.Bytes())

	for i, event := range index.evs {
		block := int(buf.PullUint64())
		offset := int(buf.PullUint16())

		if event.block != block || event.offset != offset {
			t.Errorf("Case %d: Wrong read event block/offset. wanted: %d,%d found: %d,%d", i, event.block, event.offset, block, offset)
		}
	}
}
