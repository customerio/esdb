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
	offset int64
	event  *Event
	err    error
	done   chan request
}

type Stream interface {
	Write(data []byte, indexes map[string]string) (int, error)
	ScanIndex(name, value string, offset int64, scanner Scanner) error
	Iterate(offset int64, scanner Scanner) (int64, error)
	Offset() int64
	Closed() bool
	Close() error
	reader() io.ReaderAt
	queue() chan<- request
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

func setupReadQueue(s io.ReaderAt) chan<- request {
	queue := make(chan request)

	go (func() {
		for req := range queue {
			req.event, req.err = pullEvent(s, req.offset)
			req.done <- req
		}
	})()

	return queue
}

func scanIndex(queue chan<- request, index string, offset int64, scanner Scanner) error {
	done := make(chan request)

	for offset > 0 {
		queue <- request{offset: offset, done: done}
		req := <-done

		if req.err == nil {
			offset = req.event.offsets[index]

			if !scanner(req.event) {
				offset = 0
			}
		} else {
			return req.err
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

	done := make(chan request)

	var err error

	for err == nil {
		s.queue() <- request{offset: offset, done: done}
		req := <-done

		if req.err == nil {
			offset += int64(req.event.length())

			if !scanner(req.event) {
				err = io.EOF
			}
		} else {
			err = req.err
		}
	}

	if err == io.EOF {
		return offset, nil
	} else {
		return offset, err
	}
}
