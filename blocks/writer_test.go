package blocks

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"encoding/binary"

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

	c1 := csnappy.Encode(nil, []byte("helloworldhelloworldhelloworldhe"))

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

func TestMixedBlockCompression(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 32)

	compressable := []byte{}

	for i := 0; i < 256; i++ {
		compressable = append(compressable, 0x00)
	}

	for _, b := range []byte{0xc0, 0xf2, 0x2f, 0xa2, 0x0, 0x93, 0x8b, 0x10, 0xf3, 0xbf, 0x5, 0xe4, 0xfa, 0x84, 0x37, 0x8f, 0xf2, 0xc8, 0xb0, 0xdc, 0x76, 0xe0, 0xbc, 0x35, 0x64, 0xf4, 0x3d, 0xed, 0xd4, 0xa4, 0x68, 0xfd, 0xaf} {
		compressable = append(compressable, b)
	}

	w.Write(compressable)
	w.Flush()

	var size uint

	i := 0
	encoded := []int{1, 1, 1, 1, 1, 1, 1, 1, 0, 0}

	for buffer.Len() > 0 {

		n := fixedInt(32, 0)

		if num, ok := n.(uint16); ok {
			binary.Read(buffer, binary.LittleEndian, &num)
			size = uint(num)
		}

		// Read next byte which is the block encoding.
		b, _ := buffer.ReadByte()
		encoding := int(b)

		if encoding != encoded[i] {
			t.Errorf("Encoding mismatch on write %d, expected: %d, got: %d", i, encoded[i], encoding)
		}

		block := make([]byte, size)
		n, _ = buffer.Read(block)

		i++
	}

}
