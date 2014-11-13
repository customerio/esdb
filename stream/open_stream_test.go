package stream

import (
	"errors"
	"io"
	"log"
	"os"
	"reflect"
	"testing"
)

func createStream() Stream {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.stream")

	return newStream()
}

func newStream() Stream {
	s, err := New("tmp/test.stream")
	if err != nil {
		log.Fatalf(err.Error())
	}

	return s
}

func reopenStream() Stream {
	s, err := Open("tmp/test.stream")
	if err != nil {
		log.Fatalf(err.Error())
	}

	return s
}

// byte buffer that satisfies io.ReadWriteSeeker
type RWS struct {
	buf        []byte
	off        int
	failWrites bool
}

func (b *RWS) Write(p []byte) (n int, err error) {
	if b.failWrites {
		n = len(p) / 2
		b.grow(b.off + n)
		n = copy(b.buf[b.off:], p[:n])
		b.off += n
		return n, errors.New("failed write!")
	} else {
		b.grow(b.off + len(p))
		n = copy(b.buf[b.off:], p)
		b.off += n
		return
	}
}
func (b *RWS) Read(p []byte) (n int, err error) {
	if b.off >= len(b.buf) { // Note len(nil)==0
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.off:])
	b.off += n
	return
}
func (b *RWS) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case 0:
		ret = 0
	case 1:
		ret = int64(b.off)
	case 2:
		ret = int64(len(b.buf))
	default:
		return int64(b.off), io.EOF
	}
	ret += offset
	if ret < 0 || int64(ret) != ret {
		return int64(b.off), io.EOF
	}
	b.off = int(ret)
	b.grow(b.off)
	return
}
func (b *RWS) grow(n int) {
	if n > cap(b.buf) {
		buf := make([]byte, n, n)
		copy(buf, b.buf[0:b.off])
		b.buf = buf
	}
}

func TestOpen(t *testing.T) {
	s := createStream()

	if s.Closed() {
		t.Errorf("Open stream is not open.")
	}
}

func TestTails(t *testing.T) {
	s := createStream()

	len1, _ := s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	len2, _ := s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	var tests = []struct {
		index  string
		value  string
		offset int64
	}{
		{"a", "a", int64(len(MAGIC_HEADER))},
		{"b", "b", int64(len(MAGIC_HEADER))},
		{"c", "c", int64(len(MAGIC_HEADER) + len1)},
		{"d", "d", int64(len(MAGIC_HEADER) + len1 + len2)},
		{"e", "e", int64(len(MAGIC_HEADER) + len1 + len2)},
		{"f", "f", int64(len(MAGIC_HEADER) + len1 + len2)},
	}

	for i, test := range tests {
		if off := s.(*openStream).tails[test.index+":"+test.value]; off != test.offset {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.offset, off)
		}
	}
}

func TestOpenScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	var tests = []struct {
		index  string
		value  string
		limit  int
		events []string
	}{
		{"a", "a", 100, []string{"abc"}},
		{"b", "b", 100, []string{"abc"}},
		{"c", "c", 100, []string{"cde", "abc"}},
		{"d", "d", 100, []string{"def", "cde"}},
		{"d", "d", 2, []string{"def", "cde"}},
		{"d", "d", 1, []string{"def"}},
		{"e", "e", 100, []string{"def", "cde"}},
		{"f", "f", 100, []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, test.value, 0, func(e *Event) bool {
			found = append(found, string(e.Data))
			return len(found) < test.limit
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}
}

func TestContinueScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a"})
	s.Write([]byte("cde"), map[string]string{"a": "a"})
	s.Write([]byte("def"), map[string]string{"a": "a"})

	var offset int64
	found := make([]string, 0)

	s.ScanIndex("a", "a", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("a", "a")
		return false
	})
	s.ScanIndex("a", "a", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("a", "a")
		return false
	})
	s.ScanIndex("a", "a", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("a", "a")
		return false
	})

	if offset != 0 {
		t.Errorf("Wanted offset: 0, found: %v", offset)
	}

	if !reflect.DeepEqual(found, []string{"def", "cde", "abc"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"def", "cde", "abc"}, found)
	}
}

func TestOpenIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	found := make([]string, 0)

	_, err := s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if err != nil {
		t.Errorf("Error found while iterating: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}

	found = make([]string, 0)

	_, err = s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return len(found) != 2
	})

	if err != nil {
		t.Errorf("Error found while iterating: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"abc", "cde"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde"}, found)
	}
}

func TestContinueIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a"})
	s.Write([]byte("cde"), map[string]string{"a": "a"})
	s.Write([]byte("def"), map[string]string{"a": "a"})

	var offset int64
	found := make([]string, 0)

	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})
	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})
	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})
	offset, _ = s.Iterate(offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		return false
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestReopenWrite(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})

	s = reopenStream()

	if s.Offset() > 0 {
		t.Errorf("Reopened open stream immediately initialized with offset %v", s.Offset())
	}

	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	found := make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
	if s.Offset() <= 0 {
		t.Errorf("Written open stream didn't initialize. Offset: %v", s.Offset())
	}
}

func TestReopenScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	s = reopenStream()

	if s.Offset() > 0 {
		t.Errorf("Reopened open stream immediately initialized with offset %v", s.Offset())
	}

	var tests = []struct {
		index  string
		value  string
		events []string
	}{
		{"a", "a", []string{"abc"}},
		{"b", "b", []string{"abc"}},
		{"c", "c", []string{"cde", "abc"}},
		{"d", "d", []string{"def", "cde"}},
		{"e", "e", []string{"def", "cde"}},
		{"f", "f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, test.value, 0, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}

	if s.Offset() <= 0 {
		t.Errorf("Scanned open stream didn't initialize. Offset: %v", s.Offset())
	}
}

func TestReopenIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	s = reopenStream()

	if s.Offset() > 0 {
		t.Errorf("Reopened open stream immediately initialized with offset %v", s.Offset())
	}

	found := make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}

	if s.Offset() > 0 {
		t.Errorf("Iterating on reopened open stream initialized with offset %v", s.Offset())
	}
}

func TestReopenContinueScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a"})
	s.Write([]byte("cde"), map[string]string{"a": "a"})
	s.Write([]byte("def"), map[string]string{"a": "a"})

	var offset int64
	found := make([]string, 0)

	s = reopenStream()

	if s.Offset() > 0 {
		t.Errorf("Reopened open stream immediately initialized with offset %v", s.Offset())
	}

	s.ScanIndex("a", "a", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("a", "a")
		return false
	})

	if s.Offset() <= 0 {
		t.Errorf("Scanned open stream didn't initialize. Offset: %v", s.Offset())
	}

	s = reopenStream()

	if s.Offset() > 0 {
		t.Errorf("Reopened open stream immediately initialized with offset %v", s.Offset())
	}

	s.ScanIndex("a", "a", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("a", "a")
		return false
	})

	if s.Offset() > 0 {
		t.Errorf("Scanned open stream with offset initialized with offset %v", s.Offset())
	}

	s = reopenStream()

	if s.Offset() > 0 {
		t.Errorf("Reopened open stream immediately initialized with offset %v", s.Offset())
	}

	s.ScanIndex("a", "a", offset, func(e *Event) bool {
		found = append(found, string(e.Data))
		offset = e.Next("a", "a")
		return false
	})

	if s.Offset() > 0 {
		t.Errorf("Scanned open stream with offset initialized with offset %v", s.Offset())
	}

	if offset != 0 {
		t.Errorf("Wanted offset: 0, found: %v", offset)
	}

	if !reflect.DeepEqual(found, []string{"def", "cde", "abc"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"def", "cde", "abc"}, found)
	}
}

func TestClose(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	if s.Closed() {
		t.Errorf("Open stream is not open.")
	}

	err := s.Close()
	if err != nil {
		t.Errorf("Error found while closing: %v", err)
	}

	if !s.Closed() {
		t.Errorf("Closed stream is not closed.")
	}

	s = reopenStream()

	var tests = []struct {
		index  string
		value  string
		events []string
	}{
		{"a", "a", []string{"abc"}},
		{"b", "b", []string{"abc"}},
		{"c", "c", []string{"cde", "abc"}},
		{"d", "d", []string{"def", "cde"}},
		{"e", "e", []string{"def", "cde"}},
		{"f", "f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, test.value, 0, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}

	found := make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestWritesAfterClose(t *testing.T) {
	s := createStream()

	_, err := s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	if err != nil {
		t.Errorf("Error found while writing: %v", err)
	}

	_, err = s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	if err != nil {
		t.Errorf("Error found while writing: %v", err)
	}

	_, err = s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})
	if err != nil {
		t.Errorf("Error found while writing: %v", err)
	}

	err = s.Close()
	if err != nil {
		t.Errorf("Error found while closing: %v", err)
	}

	_, err = s.Write([]byte("efg"), map[string]string{"e": "e", "f": "f", "g": "g"})
	if err == nil {
		t.Errorf("No error found while writing to closed stream.")
	}
}

func TestInterleavedReadWrites(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})

	found := make([]string, 0)

	s.ScanIndex("c", "c", 0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"cde", "abc"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"cde", "abc"}, found)
	}

	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	s = reopenStream()

	found = make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestRecoverOpenCorruptedLog(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})

	// Write some bad data to the end of the log file.
	s.(*openStream).stream.Write([]byte("rawr!"))

	s = reopenStream()

	found := make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestFailedWrite(t *testing.T) {
	rws := &RWS{buf: make([]byte, 0)}
	s, _ := createOpenStream(rws)

	n, err := s.Write([]byte("abc"), map[string]string{"a": "a", "b": "b", "c": "c"})
	if n != 24 || err != nil {
		t.Errorf("Write incorrect results. expected: 24, <nil> found: %v, %v", n, err)
	}

	n, err = s.Write([]byte("cde"), map[string]string{"c": "c", "d": "d", "e": "e"})
	if n != 24 || err != nil {
		t.Errorf("Write incorrect results. expected: 24, <nil> found: %v, %v", n, err)
	}

	n, err = s.Write([]byte("def"), map[string]string{"d": "d", "e": "e", "f": "f"})
	if n != 24 || err != nil {
		t.Errorf("Write incorrect results. expected: 24, <nil> found: %v, %v", n, err)
	}

	rws.failWrites = true
	n, err = s.Write([]byte("efg"), map[string]string{"e": "e", "f": "f", "g": "g"})
	if n != 0 || err == nil {
		t.Errorf("Write incorrect results. expected: 0, !<nil> found: %v, %v", n, err)
	}
	rws.failWrites = false

	n, err = s.Write([]byte("fgh"), map[string]string{"f": "f", "g": "g", "h": "h"})
	if n != 24 || err != nil {
		t.Errorf("Write incorrect results. expected: 24, <nil> found: %v, %v", n, err)
	}

	found := make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def", "fgh"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def", "fgh"}, found)
	}

	// Let's write out the data in the RWS to a file
	// and see if we can open it and read past the
	// failed write.
	log := createStream()
	n, err = log.(*openStream).stream.Write(rws.buf[len(MAGIC_HEADER):])
	err = log.(*openStream).stream.(io.Closer).Close()

	s = reopenStream()

	found = make([]string, 0)

	s.Iterate(0, func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def", "fgh"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def", "fgh"}, found)
	}
}
