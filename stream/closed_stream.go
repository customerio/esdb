package stream

import (
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/bounded"
	"github.com/customerio/esdb/sst"
)

var WRITING_TO_CLOSED_STREAM = errors.New("stream has been closed")

type closedStream struct {
	stream io.ReaderAt
	index  *sst.Reader
	readq  chan<- request
}

func readonly(path string) (Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	header := binary.ReadBytes(file, int64(len(MAGIC_HEADER)))

	if string(header) != string(MAGIC_HEADER) {
		return nil, CORRUPTED_HEADER
	}

	return newClosedStream(file)
}

func newClosedStream(stream *os.File) (Stream, error) {
	index, err := findIndex(stream)
	if err != nil {
		return nil, err
	}

	return &closedStream{
		stream: stream,
		index:  index,
		readq:  setupReadQueue(stream),
	}, nil
}

func (s *closedStream) Write(data []byte, indexes map[string]string) (int, error) {
	return 0, WRITING_TO_CLOSED_STREAM
}

func (s *closedStream) ScanIndex(name, value string, offset int64, scanner Scanner) error {
	index := name + ":" + value

	if offset <= 0 {
		val, err := s.index.Get([]byte(index))
		if err != nil {
			if err.Error() == "not found" {
				return nil
			} else {
				return err
			}
		}

		b := bytes.NewReader(val)
		offset = binary.ReadUvarint(b)
	}

	return scanIndex(s.readq, index, offset, scanner)
}

func (s *closedStream) Iterate(offset int64, scanner Scanner) (int64, error) {
	return iterate(s, offset, scanner)
}

func (s *closedStream) Offset() int64 {
	return 0
}

func (s *closedStream) Closed() bool {
	return true
}

func (s *closedStream) Close() error {
	close(s.readq)

	if closer, ok := s.stream.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

func (s *closedStream) reader() io.ReaderAt {
	return s.stream
}

func (s *closedStream) queue() chan<- request {
	return s.readq
}

func findIndex(f *os.File) (*sst.Reader, error) {
	// The last 8 bytes in the file is the length
	// of the SSTable spaces index.
	f.Seek(-FOOTER_LENGTH-8, 2)
	indexLen := binary.ReadInt64(f)

	return sst.NewReader(bounded.New(f, -8-FOOTER_LENGTH-indexLen, -8-FOOTER_LENGTH), indexLen)
}
