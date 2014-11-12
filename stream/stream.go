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

type Stream interface {
	Write(data []byte, indexes map[string]string) (int, error)
	ScanIndex(name, value string, offset int64, scanner Scanner) error
	Iterate(offset int64, scanner Scanner) (int64, error)
	Offset() int64
	Closed() bool
	Close() error
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

func scanIndex(stream io.ReadSeeker, index string, offset int64, scanner Scanner) error {
	for offset > 0 {
		stream.Seek(offset, 0)

		if event, err := pullEvent(stream); err == nil {
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

func iterate(stream io.ReadSeeker, offset int64, scanner Scanner) (int64, error) {
	if offset <= 0 {
		stream.Seek(0, 0)

		header := binary.ReadBytes(stream, HEADER_LENGTH)

		if string(header) != string(MAGIC_HEADER) {
			return 0, CORRUPTED_HEADER
		}

		offset += HEADER_LENGTH
	} else {
		stream.Seek(offset, 0)
	}

	var event *Event
	var err error

	for err == nil {
		if event, err = pullEvent(stream); err == nil {
			offset += int64(event.length())

			if !scanner(event) {
				err = io.EOF
			}
		}
	}

	if err == io.EOF {
		return offset, nil
	} else {
		return offset, err
	}
}
