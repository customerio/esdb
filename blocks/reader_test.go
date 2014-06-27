package blocks

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestRead(t *testing.T) {
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
		buffer := new(bytes.Buffer)
		w := NewWriter(buffer, test.blockSize)

		w.Write([]byte(test.input))
		w.Flush()

		r := NewReader(bytes.NewReader(buffer.Bytes()), test.blockSize)

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
	}
}

func TestSeek(t *testing.T) {
	buffer := new(bytes.Buffer)
	w := NewWriter(buffer, 5)

	w.Write([]byte("abcdefghijklmnopqrstuvwxyz"))
	w.Flush()

	r := NewReader(bytes.NewReader(buffer.Bytes()), 5)

	var tests = []struct {
		position int
		result   string
	}{
		{5, "def"},
		{0, "abc"},
		{10, "ghi"},
		{5, "def"},
		{20, "mno"},
		{0, "abc"},
		{50, ""},
	}

	for i, test := range tests {
		bytes := make([]byte, len(test.result))

		r.Seek(int64(test.position), 0)
		r.Read(bytes)

		if !reflect.DeepEqual(bytes, []byte(test.result)) {
			t.Errorf("Wrong bytes for Case %d:\n want: %s\n  got: %s", i, test.result, bytes)
		}
	}

	if n, err := r.Seek(5, 0); n != 5 || err != nil {
		t.Errorf("Wrong return:\n want: 5,<nil>\n  got: %d,%v", n, err)
	}

	if n, err := r.Seek(5, 1); n != 0 || err == nil {
		t.Errorf("Wrong return:\n want: 0,block reader can only seek relative to beginning of file.\n  got: %d,%v", n, err)
	}

	if n, err := r.Seek(5, 2); n != 0 || err == nil {
		t.Errorf("Wrong return:\n want: 0,block reader can only seek relative to beginning of file.\n  got: %d,%v", n, err)
	}
}
