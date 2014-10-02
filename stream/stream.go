package stream

import (
	"io"
	"os"

	"github.com/customerio/esdb/binary"
)

const (
	MAGIC_HEADER = "\x65\x73\x64\x62\x73\x74\x72\x65\x61\x6d"
	MAGIC_FOOTER = "\x63\x6c\x6f\x73\x65\x64\x65\x73\x64\x62\x73\x74\x72\x65\x61\x6d"
)

var HEADER_LENGTH = int64(len(MAGIC_HEADER))
var FOOTER_LENGTH = int64(len(MAGIC_FOOTER))

type Scanner func(*Event) bool

type Stream interface {
	Write(data []byte, indexes map[string]string) (int, error)
	ScanIndex(index, value string, scanner Scanner) error
	Iterate(scanner Scanner) error
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
			scanner(event)
			offset = event.offsets[index]
		} else {
			return err
		}
	}

	return nil
}

func iterate(stream io.ReadSeeker, scanner Scanner) error {
	stream.Seek(0, 0)

	header := binary.ReadBytes(stream, HEADER_LENGTH)

	if string(header) != string(MAGIC_HEADER) {
		return CORRUPTED_HEADER
	}

	var event *Event
	var err error

	for err == nil {
		if event, err = pullEvent(stream); err == nil {
			if !scanner(event) {
				err = io.EOF
			}
		}
	}

	if err == io.EOF {
		return nil
	} else {
		return err
	}
}
