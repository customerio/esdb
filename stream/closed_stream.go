package stream

import (
	"errors"
	"io"
	"os"

	"github.com/customerio/esdb/sst"
)

type closedStream struct {
	file  io.ReadSeeker
	index *sst.Reader
}

func Readonly(path string) (Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	index, err := findIndex(file)
	if err != nil {
		return nil, err
	}

	return &closedStream{
		file:  file,
		index: index,
	}, nil
}

func (s *closedStream) Write(data []byte, indexes []string) (int, error) {
	return 0, errors.New("stream already closed")
}

func (s *closedStream) ScanIndex(index string, scanner Scanner) error {
	// starts at the offset stored in the sst index
	// reads event and sends it to scanner
	// while event has a previous offset
	//   read event at previous offset
	//   send to scanner
	return nil
}

func (s *closedStream) Iterate(scanner Scanner) error {
	// Reads all events from file
	return nil
}

func (s *closedStream) Closed() bool {
	return true
}

func (s *closedStream) Close() error {
	// If stream is open, write tails into
	// an sst table at the end of the file
	return errors.New("stream already closed")
}

func findIndex(f io.ReadSeeker) (*sst.Reader, error) {
	//// The last 8 bytes in the file is the length
	//// of the SSTable spaces index.
	//f.Seek(-8, 2)
	//indexLen := binary.ReadInt64(f)

	//return sst.NewReader(newBoundReader(f, -8-indexLen, -8), indexLen)
	return nil, nil
}
