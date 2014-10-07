package bounded

import (
	"bytes"
	"testing"
)

func TestBoundRead(t *testing.T) {
	contents := []byte("abcdefghijklmnopqrstuvwxyz")

	var tests = []struct {
		offset   int64
		length   int64
		read     int
		expected string
	}{
		{0, 30, 30, "abcdefghijklmnopqrstuvwxyz"},
		{0, 5, 30, "abcde"},
		{0, 5, 3, "abc"},
		{21, 5, 30, "vwxyz"},
		{21, 5, 3, "vwx"},
		{21, 5, 5, "vwxyz"},
		{10, 5, 30, "klmno"},
		{10, 5, 3, "klm"},
		{-6, 5, 30, "uvwxy"},
		{-6, 5, 3, "uvw"},
		{-6, 5, 3, "uvw"},
	}

	for i, test := range tests {
		reader := New(bytes.NewReader(contents), test.offset, test.offset+test.length)

		data := make([]byte, test.read)
		n, _ := reader.Read(data)

		if string(data[:n]) != test.expected {
			t.Errorf("Case #%d wrong content: wanted: %s, found: %s", i, test.expected, string(data[:n]))
		}
	}
}

func TestBoundSeek(t *testing.T) {
	contents := []byte("abcdefghijklmnopqrstuvwxyz")

	var tests = []struct {
		offset   int64
		length   int64
		seek     int64
		read     int
		expected string
	}{
		{0, 30, 5, 30, "fghijklmnopqrstuvwxyz"},
		{0, 5, 10, 30, ""},
		{0, 5, 3, 1, "d"},
		{21, 5, 3, 1, "y"},
		{21, 5, -3, 1, "x"},
		{21, 5, 5, 1, ""},
		{21, 5, 0, 1, "v"},
		{10, 5, 3, 5, "no"},
		{-6, 5, -6, 30, "uvwxy"},
		{-6, 5, -2, 3, "xy"},
		{-6, 5, -8, 3, "uvw"},
		{-6, 2, -1, 30, "v"},
		{-6, 2, 1, 30, "v"},
		{-6, 2, -2, 3, "uv"},
		{-6, 2, -8, 3, "uv"},
	}

	for i, test := range tests {
		reader := New(bytes.NewReader(contents), test.offset, test.offset+test.length)

		if test.seek < 0 {
			reader.Seek(test.seek, 2)
		} else {
			reader.Seek(test.seek, 0)
		}

		data := make([]byte, test.read)
		n, _ := reader.Read(data)

		if string(data[:n]) != test.expected {
			t.Errorf("Case #%d wrong content: wanted: %s, found: %s", i, test.expected, string(data[:n]))
		}
	}
}
