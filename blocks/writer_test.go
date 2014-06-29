package blocks

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/dgryski/go-csnappy"
)

func ExampleWriter() {
	buffer := new(bytes.Buffer)
	writer := NewWriter(buffer, 5)

	writer.Write([]byte("hello world"))
	writer.Flush()

	fmt.Printf("%q", buffer.Bytes())

	// Output: "\x05\x00\x00hello\x05\x00\x00 worl\x01\x00\x00d"
}

func TestWriterSmallBlockSize(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 32)

	for i, expected := range []int{0, 0, 0, 18} {
		if n, err := w.Write([]byte("helloworld")); n != expected || err != nil {
			t.Errorf("Wrong response for write %d: want: %d,<nil> got: %d,%v", i, expected, n, err)
		}
	}

	if n, err := w.Flush(); n != 11 || err != nil {
		t.Errorf("Wrong response for flush: want: 11,<nil> got: %d,%v", n, err)
	}

	c1, _ := csnappy.Encode(nil, []byte("helloworldhelloworldhelloworldhe"))

	expected := []byte(
		"\x0f\x00\x01" + string(c1) + "\x08\x00\x00lloworld",
	)

	if !reflect.DeepEqual(buffer.Bytes(), expected) {
		t.Errorf("Wrong block formatting:\n want: %x\n  got: %x", expected, buffer.Bytes())
	}
}

func TestWriterTinyBlockSize(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 2)

	if n, err := w.Write([]byte("helloworld")); n != 20 || err != nil {
		t.Errorf("Wrong response for write: want: 20,<nil> got: %d,%v", n, err)
	}

	if n, err := w.Flush(); n != 5 || err != nil {
		t.Errorf("Wrong response for flush: want: 5,<nil> got: %d,%v", n, err)
	}

	expected := []byte(
		"\x02\x00\x00he\x02\x00\x00ll\x02\x00\x00ow\x02\x00\x00or\x02\x00\x00ld",
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

	if buffer.Len() != 6191 {
		t.Errorf("Wrong written length: want: 6191 got: %d", buffer.Len())
	}

	expected := []byte("\x19\x18\x00\x00\x01")

	if !reflect.DeepEqual(buffer.Bytes()[:5], expected) {
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
		{"ghi", 3, 9, 1},
		{"jkl", 6, 9, 1},
		{"lmn", 3, 18, 2},
		{"opq", 6, 18, 2},
		{"", 0, 27, 3},
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
