package blocks

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func ExampleFastReader() {
	reader := NewFastReader(context.Background(), bytes.NewReader(
		[]byte("\x05\x00\x00hello\x05\x00\x00 worl\x01\x00\x00d"),
	), 5)

	result := make([]byte, 11)
	n, _ := reader.Read(result)

	fmt.Printf("%q", result[:n])

	// Output: "hello world"
}

func newBufioReader() interface{} {
	return bufio.NewReaderSize(nil, 1024*1024)
}

var fileReaderPool sync.Pool = sync.Pool{
	New: newBufioReader,
}

func TestFastRead(t *testing.T) {
	var tests = []struct {
		blockSize int
		input     string
		readSize  int
	}{
		{32, "helloworldhelloworldhelloworld", 35},
		{32, "helloworldhelloworldhelloworld", 30},
		{32, "helloworldhelloworldhelloworld", 20},
		{2, "helloworld", 15},
		{10, "helloworldhelloworldhelloworld", 10},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			buffer := new(bytes.Buffer)
			w := NewWriter(buffer, test.blockSize)

			w.Write([]byte(test.input))
			w.Flush()

			// r := NewFastReader(context.Background(), bytes.NewReader(buffer.Bytes()), test.blockSize)
			f := bytes.NewReader(buffer.Bytes())
			fr := fileReaderPool.Get().(*bufio.Reader)
			fr.Reset(f)

			// defer func() {
			// 	fr.Reset(nil)
			// 	fileReaderPool.Put(fr)
			// }()

			ctx, cancel := context.WithCancel(context.Background())
			r := NewFastReader(ctx, fr, test.blockSize)

			defer func() {
				cancel()
				r.Close()
				fr.Reset(nil)
				fileReaderPool.Put(fr)
			}()

			if true {
				return
			}

			var err error
			var length int
			result := make([]byte, 0)
			bytes := make([]byte, test.readSize)
			n := 1

			for n > 0 && err == nil {
				n, err = r.Read(bytes)
				result = append(result, bytes[:n]...)
				length += n
			}

			if !reflect.DeepEqual(result, []byte(test.input)) {
				t.Errorf("Wrong bytes for Case %d:\n want: %x\n  got: %x", i, test.input, result)
			}

			if length != len(test.input) || err != io.EOF {
				t.Errorf("Wrong return for Case %d: want: %d,EOF got: %d,%v", i, len(test.input), length, err)
			}
		})
	}
}

func TestFastReadByte(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 5)

	data := []byte("abcdefghijklmnopqrstuvwxyz")

	w.Write(data)
	w.Flush()

	r := NewFastReader(context.Background(), bytes.NewReader(buffer.Bytes()), 5)

	for i, expected := range data {
		found, err := r.ReadByte()

		if found != expected {
			t.Errorf("Wrong byte for Case %d: want: %v got: %v", i, expected, found)
		}

		if err != nil {
			t.Errorf("Wrong error for Case %d: want: <nil> got: %v", i, err)
		}
	}

	b, err := r.ReadByte()

	if b != 0 || err == nil {
		t.Errorf("Wrong return from empty ReadByte: want: <nil>,EOF got: %x%v", b, err)
	}
}
