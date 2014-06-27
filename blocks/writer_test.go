package blocks

import (
	"bytes"
	"reflect"
	"testing"
)

func TestWriterSmallBlockSize(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 32)

	for i, expected := range []int{0, 0, 0, 34} {
		if n, err := w.Write([]byte("helloworld")); n != expected || err != nil {
			t.Errorf("Wrong response for write %d: want: %d,<nil> got: %d,%v", i, expected, n, err)
		}
	}

	if n, err := w.Flush(); n != 10 || err != nil {
		t.Errorf("Wrong response for flush: want: 10,<nil> got: %d,%v", n, err)
	}

	expected := []byte(
		"\x20\x00helloworldhelloworldhelloworldhe\x08\x00lloworld",
	)

	if !reflect.DeepEqual(buffer.Bytes(), expected) {
		t.Errorf("Wrong block formatting:\n want: %x\n  got: %x", expected, buffer.Bytes())
	}
}

func TestWriterTinyBlockSize(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 2)

	if n, err := w.Write([]byte("helloworld")); n != 16 || err != nil {
		t.Errorf("Wrong response for write: want: 16,<nil> got: %d,%v", n, err)
	}

	if n, err := w.Flush(); n != 4 || err != nil {
		t.Errorf("Wrong response for flush: want: 4,<nil> got: %d,%v", n, err)
	}

	expected := []byte(
		"\x02\x00he\x02\x00ll\x02\x00ow\x02\x00or\x02\x00ld",
	)

	if !reflect.DeepEqual(buffer.Bytes(), expected) {
		t.Errorf("Wrong block formatting:\n want: %x\n  got: %x", expected, buffer.Bytes())
	}
}

func TestWriterLargeBlockSize(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 131068)

	for i := 0; i < 13108; i++ {
		if _, err := w.Write([]byte("helloworld")); err != nil {
			t.Fatalf("Wrong response for write %d: want: <nil> got: %v", i, err)
		}
	}

	if _, err := w.Flush(); err != nil {
		t.Errorf("Wrong response for flush: want: <nil> got: %v", err)
	}

	if buffer.Len() != 131088 {
		t.Errorf("Wrong written length: want: 131088 got: %d", buffer.Len())
	}

	expected := []byte(
		"\xfc\xff\x01\x00helloworldhellow",
	)

	if !reflect.DeepEqual(buffer.Bytes()[:20], expected) {
		t.Errorf("Wrong block formatting:\n want: %x\n  got: %x", expected, buffer.Bytes()[:20])
	}
}

func TestWriterState(t *testing.T) {
	var tests = []struct {
		write    string
		buffered int
		written  int
		blocks   int
	}{
		{"abc", 3, 0, 0},
		{"def", 6, 0, 0},
		{"ghi", 3, 8, 1},
		{"jkl", 6, 8, 1},
		{"lmn", 3, 16, 2},
		{"opq", 6, 16, 2},
		{"", 0, 24, 3},
	}

	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 6)

	for i, test := range tests {
		if len(test.write) > 0 {
			w.Write([]byte(test.write))
		} else {
			w.Flush()
		}

		if w.Buffered() != test.buffered {
			t.Errorf("Wrong buffered for write %d: want: %d got: %d", i, test.buffered, w.Buffered())
		}

		if w.Written != test.written {
			t.Errorf("Wrong written for write %d: want: %d got: %d", i, test.written, w.Written)
		}

		if w.Blocks != test.blocks {
			t.Errorf("Wrong blocks for write %d: want: %d got: %d", i, test.blocks, w.Blocks)
		}
	}
}
