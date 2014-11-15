package stream

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/sst"
)

var CORRUPTED_HEADER = errors.New("Incorrect stream file header.")

type openStream struct {
	stream   Streamer
	tails    map[string]int64
	closed   bool
	offset   int64
	length   int
	initlock sync.Once
}

func read(path string) (Stream, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	return newOpenStream(file), nil
}

func createOpenStream(stream Streamer) (Stream, error) {
	offset, err := stream.WriteAt([]byte(MAGIC_HEADER), 0)
	if err != nil {
		return nil, err
	}

	return &openStream{
		stream: stream,
		tails:  make(map[string]int64),
		offset: int64(offset),
	}, nil
}

func newOpenStream(stream Streamer) Stream {
	return &openStream{stream: stream}
}

func Serialize(data []byte, indexes map[string]string, tails map[string]int64) ([]byte, error) {
	offsets := make(map[string]int64)

	for name, value := range indexes {
		index := name + ":" + value

		if off, ok := tails[index]; ok {
			offsets[index] = off
		} else {
			offsets[index] = 0
		}
	}

	event := newEvent(data, offsets)

	buf := bytes.NewBuffer([]byte{})

	_, err := event.push(buf)
	if err != nil {
		return []byte{}, err
	}

	return buf.Bytes(), nil
}

func (s *openStream) Write(data []byte, indexes map[string]string) (int, error) {
	if s.Closed() {
		return 0, WRITING_TO_CLOSED_STREAM
	}

	if err := s.init(); err != nil {
		return 0, err
	}

	bytes, err := Serialize(data, indexes, s.tails)
	if err != nil {
		return 0, err
	}

	written, err := s.stream.WriteAt(bytes, s.offset)
	if err != nil {
		return 0, err
	}

	for name, value := range indexes {
		index := name + ":" + value
		s.tails[index] = s.offset
	}

	s.offset += int64(written)
	s.length += 1

	return written, nil
}

func (s *openStream) ScanIndex(name, value string, offset int64, scanner Scanner) error {
	index := name + ":" + value

	if offset <= 0 {
		if err := s.init(); err != nil {
			return err
		}

		offset = s.tails[index]
	}

	return scanIndex(s, index, offset, scanner)
}

func (s *openStream) Iterate(offset int64, scanner Scanner) (int64, error) {
	return iterate(s, offset, scanner)
}

func (s *openStream) Offset() int64 {
	return s.offset
}

func (s *openStream) Closed() bool {
	return s.closed
}

func (s *openStream) reader() io.ReaderAt {
	return s.stream
}

func (s *openStream) Close() (err error) {
	if s.Closed() {
		return
	}

	err = s.init()
	if err != nil {
		return err
	}

	// Write nil event, to signal end of events.
	binary.WriteInt32At(s.stream, 0, s.offset)
	s.offset += 4

	indexes := make(sort.StringSlice, 0, len(s.tails))

	for name, _ := range s.tails {
		indexes = append(indexes, name)
	}

	sort.Stable(indexes)

	buf := new(bytes.Buffer)
	st := sst.NewWriter(buf)

	// For each grouping or index, we index the section's
	// byte offset in the file and the length in bytes
	// of all data in the grouping/index.
	for _, name := range indexes {
		buf := new(bytes.Buffer)

		binary.WriteUvarint64(buf, s.tails[name])

		if err = st.Set([]byte(name), buf.Bytes()); err != nil {
			return
		}
	}

	if err = st.Close(); err != nil {
		return
	}

	binary.WriteInt64(buf, int64(len(buf.Bytes())))
	buf.Write([]byte(MAGIC_FOOTER))

	_, err = s.stream.WriteAt(buf.Bytes(), s.offset)
	if err == nil {
		s.closed = true
	}

	if closer, ok := s.stream.(io.Closer); ok {
		return closer.Close()
	}

	return
}

func (s *openStream) init() (e error) {
	s.initlock.Do(func() {
		tails, offset, length, err := populate(s)

		e = err

		if e == nil {
			s.tails = tails
			s.offset = offset
			s.length = length
		}
	})

	return
}

func populate(s *openStream) (tails map[string]int64, offset int64, length int, err error) {
	tails = make(map[string]int64)
	offset = HEADER_LENGTH

	_, err = iterate(s, 0, func(event *Event) bool {
		for index, _ := range event.offsets {
			tails[index] = offset
		}

		// set tail for all event indexes
		offset += int64(event.length())
		length += 1

		return true
	})

	// If we couldn't decode the last event, it's ok.
	if err == CORRUPTED_EVENT {
		err = nil
	}

	return
}
