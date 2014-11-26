package stream

import (
	"io"
	"os"

	"github.com/customerio/esdb/binary"
)

const (
	MAGIC_HEADER = "ESDBstream"
	MAGIC_FOOTER = "closedESDBstream"
)

var HEADER_LENGTH = int64(len(MAGIC_HEADER))
var FOOTER_LENGTH = int64(len(MAGIC_FOOTER))

type Scanner func(*Event) bool

type Streamer interface {
	io.WriterAt
	io.ReaderAt
}

type request struct {
	reader io.ReaderAt
	offset int64
	event  *Event
	err    error
}

type Stream interface {
	Write(data []byte, indexes map[string]string) (int, error)
	First(name, value string) (int64, error)
	ScanIndex(name, value string, offset int64, scanner Scanner) error
	Iterate(offset int64, scanner Scanner) (int64, error)
	Offset() int64
	Closed() bool
	Close() error
	reader() io.ReaderAt
}

// Creates a new open stream at the given path. If the
// file already exists, an error will be returned.
func New(path string) (Stream, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, err
	}

	return createOpenStream(file)
}

func Open(path string) (Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	file.Seek(-FOOTER_LENGTH, 2)
	footer := binary.ReadBytes(file, FOOTER_LENGTH)
	file.Close()

	// 2 states a stream file can be in:
	// Closed: if footer is present, no additional writes allowed, read-only.
	// Open:   repopulate indexes into memory, allow additional writes.
	if string(footer) == string(MAGIC_FOOTER) {
		return readonly(path)
	} else {
		return read(path)
	}
}

func scanIndex(s Stream, index string, offset int64, scanner Scanner) error {
	for offset > 0 {
		event, err := pullEvent(s.reader(), offset)

		if err == nil {
			offset = event.offsets[index]

			if !scanner(event) {
				offset = 0
			}
		} else {
			return err
		}
	}

	return nil
}

func iterate(s Stream, offset int64, scanner Scanner) (int64, error) {
	if offset <= 0 {
		header := binary.ReadBytesAt(s.reader(), HEADER_LENGTH, 0)

		if string(header) != string(MAGIC_HEADER) {
			return 0, CORRUPTED_HEADER
		}

		offset = HEADER_LENGTH
	}

	var err error

	for err == nil {
		event, e := pullEvent(s.reader(), offset)

		if e == nil {
			offset += int64(event.length())

			if !scanner(event) {
				err = io.EOF
			}
		} else {
			err = e
		}
	}

	if err == io.EOF {
		return offset, nil
	} else {
		return offset, err
	}
}
