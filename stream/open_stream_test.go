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
	if ret < 0 || int64(ret) != ret { // BUG: what to do if int64 cannot fit int
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

func TestTails(t *testing.T) {
	s := createStream()

	len1, _ := s.Write([]byte("abc"), []string{"a", "b", "c"})
	len2, _ := s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	var tests = []struct {
		index  string
		offset int64
	}{
		{"a", int64(len(MAGIC_HEADER))},
		{"b", int64(len(MAGIC_HEADER))},
		{"c", int64(len(MAGIC_HEADER) + len1)},
		{"d", int64(len(MAGIC_HEADER) + len1 + len2)},
		{"e", int64(len(MAGIC_HEADER) + len1 + len2)},
		{"f", int64(len(MAGIC_HEADER) + len1 + len2)},
	}

	for i, test := range tests {
		if off := s.(*openStream).tails[test.index]; off != test.offset {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.offset, off)
		}
	}
}

func TestScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	var tests = []struct {
		index  string
		events []string
	}{
		{"a", []string{"abc"}},
		{"b", []string{"abc"}},
		{"c", []string{"cde", "abc"}},
		{"d", []string{"def", "cde"}},
		{"e", []string{"def", "cde"}},
		{"f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}
}

func TestIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	found := make([]string, 0)

	err := s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if err != nil {
		t.Errorf("Error found while iterating: %v", err)
	}

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestReopenScan(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	s = reopenStream()

	var tests = []struct {
		index  string
		events []string
	}{
		{"a", []string{"abc"}},
		{"b", []string{"abc"}},
		{"c", []string{"cde", "abc"}},
		{"d", []string{"def", "cde"}},
		{"e", []string{"def", "cde"}},
		{"f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)

		s.ScanIndex(test.index, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}
	}
}

func TestReopenIterate(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	s = reopenStream()

	found := make([]string, 0)

	s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestClose(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	err := s.Close()
	if err != nil {
		t.Errorf("Error found while closing: %v", err)
	}

	s2 := reopenStream()

	var tests = []struct {
		index  string
		events []string
	}{
		{"a", []string{"abc"}},
		{"b", []string{"abc"}},
		{"c", []string{"cde", "abc"}},
		{"d", []string{"def", "cde"}},
		{"e", []string{"def", "cde"}},
		{"f", []string{"def"}},
	}

	for i, test := range tests {
		found := make([]string, 0)
		found2 := make([]string, 0)

		s.ScanIndex(test.index, func(e *Event) bool {
			found = append(found, string(e.Data))
			return true
		})

		s2.ScanIndex(test.index, func(e *Event) bool {
			found2 = append(found2, string(e.Data))
			return true
		})

		if !reflect.DeepEqual(found, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found)
		}

		if !reflect.DeepEqual(found2, test.events) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.events, found2)
		}
	}

	found := make([]string, 0)
	found2 := make([]string, 0)

	s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	s2.Iterate(func(e *Event) bool {
		found2 = append(found2, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}

	if !reflect.DeepEqual(found2, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found2)
	}
}

func TestWritesAfterClose(t *testing.T) {
	s := createStream()

	_, err := s.Write([]byte("abc"), []string{"a", "b", "c"})
	if err != nil {
		t.Errorf("Error found while writing: %v", err)
	}

	_, err = s.Write([]byte("cde"), []string{"c", "d", "e"})
	if err != nil {
		t.Errorf("Error found while writing: %v", err)
	}

	_, err = s.Write([]byte("def"), []string{"d", "e", "f"})
	if err != nil {
		t.Errorf("Error found while writing: %v", err)
	}

	err = s.Close()
	if err != nil {
		t.Errorf("Error found while closing: %v", err)
	}

	_, err = s.Write([]byte("efg"), []string{"e", "f", "g"})
	if err == nil {
		t.Errorf("No error found while writing to closed stream.")
	}
}

func TestInterleavedReadWrites(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})

	found := make([]string, 0)

	s.ScanIndex("c", func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"cde", "abc"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"cde", "abc"}, found)
	}

	s.Write([]byte("def"), []string{"d", "e", "f"})

	s = reopenStream()

	found = make([]string, 0)

	s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def"}, found)
	}
}

func TestRecoverCorruptedLog(t *testing.T) {
	s := createStream()

	s.Write([]byte("abc"), []string{"a", "b", "c"})
	s.Write([]byte("cde"), []string{"c", "d", "e"})
	s.Write([]byte("def"), []string{"d", "e", "f"})

	// Write some bad data to the end of the log file.
	s.(*openStream).stream.Write([]byte("rawr!"))

	s = reopenStream()

	found := make([]string, 0)

	s.Iterate(func(e *Event) bool {
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

	n, err := s.Write([]byte("abc"), []string{"a", "b", "c"})
	if n != 18 || err != nil {
		t.Errorf("Write incorrect results. found: %v, %v", n, err)
	}

	n, err = s.Write([]byte("cde"), []string{"c", "d", "e"})
	if n != 18 || err != nil {
		t.Errorf("Write incorrect results. found: %v, %v", n, err)
	}

	n, err = s.Write([]byte("def"), []string{"d", "e", "f"})
	if n != 18 || err != nil {
		t.Errorf("Write incorrect results. found: %v, %v", n, err)
	}

	rws.failWrites = true
	n, err = s.Write([]byte("efg"), []string{"e", "f", "g"})
	if n != 0 || err == nil {
		t.Errorf("Write incorrect results. found: %v, %v", n, err)
	}
	rws.failWrites = false

	n, err = s.Write([]byte("fgh"), []string{"f", "g", "h"})
	if n != 18 || err != nil {
		t.Errorf("Write incorrect results. found: %v, %v", n, err)
	}

	found := make([]string, 0)

	s.Iterate(func(e *Event) bool {
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

	s.Iterate(func(e *Event) bool {
		found = append(found, string(e.Data))
		return true
	})

	if !reflect.DeepEqual(found, []string{"abc", "cde", "def", "fgh"}) {
		t.Errorf("Wanted: %v, found: %v", []string{"abc", "cde", "def", "fgh"}, found)
	}
}
