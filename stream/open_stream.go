package stream

import (
	"errors"
	"io"
	"os"

	"github.com/customerio/esdb/binary"
)

const (
	MAGIC = "\x73\x74\x72\x65\x61\x6d" //\x57\xfb\x80\x8b\x24\x75\x47\xdb"
)

type openStream struct {
	stream io.ReadWriteSeeker
	tails  map[string]int64
	offset int64
	length int
}

// Creates a new open stream at the given path. If the
// file already exists, an error will be returned.
func New(path string) (Stream, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, err
	}

	offset, err := file.Write([]byte(MAGIC))
	if err != nil {
		return nil, err
	}

	return &openStream{
		stream: file,
		tails:  make(map[string]int64),
		offset: int64(offset),
	}, nil
}

// Opens an existing open stream, populates tails,
// offset, etc from written events.
func Read(path string) (Stream, error) {
	stream, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	s := &openStream{stream: stream}

	tails, offset, length, err := scan(s.stream)

	s.tails = tails
	s.offset = offset
	s.length = length

	return s, err
}

func (s *openStream) Write(data []byte, indexes []string) (int, error) {
	offsets := make(map[string]int64)

	for _, index := range indexes {
		if off, ok := s.tails[index]; ok {
			offsets[index] = off
		} else {
			offsets[index] = 0
		}
	}

	event := newEvent(data, offsets)

	written, err := event.push(s.stream)
	if err != nil {
		return written, err
	}

	for _, index := range indexes {
		s.tails[index] = s.offset
	}

	s.offset += int64(written)
	s.length += 1

	return written, nil
}

func (s *openStream) ScanIndex(index string, scanner Scanner) error {
	off := s.tails[index]

	for off > 0 {
		s.stream.Seek(off, 0)

		if event, err := pullEvent(s.stream); err == nil {
			scanner(event)
			off = event.offsets[index]
		} else {
			return err
		}
	}

	return nil
}

func (s *openStream) Iterate(scanner Scanner) error {
	s.stream.Seek(int64(len(MAGIC)), 0)

	var event *Event
	var err error

	for err == nil {
		if event, err = pullEvent(s.stream); err == nil {
			scanner(event)
		}
	}

	return nil
}

func (s *openStream) Closed() bool {
	return false
}

func (s *openStream) Close() error {
	// If stream is open, write tails into
	// an sst table at the end of the file
	return nil
}

func scan(stream io.Reader) (tails map[string]int64, offset int64, length int, err error) {
	tails = make(map[string]int64)

	var event *Event

	header := binary.ReadBytes(stream, int64(len(MAGIC)))

	if string(header) != string(MAGIC) {
		err = errors.New("Incorrect stream file header.")
		return
	}

	offset += int64(len(header))

	for event, err = pullEvent(stream); err == nil; event, err = pullEvent(stream) {
		for index, _ := range event.offsets {
			tails[index] = offset
		}

		// set tail for all event indexes
		offset += int64(event.length())
		length += 1
	}

	if err == io.EOF {
		err = nil
	}

	return
}
