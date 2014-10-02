package stream

import (
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
	Write(data []byte, indexes []string) (int, error)
	ScanIndex(index string, scanner Scanner) error
	Iterate(scanner Scanner) error
	Closed() bool
	Close() error
}

func Open(path string) (Stream, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// 2 states a stream file can be in:
	// Open: repopulate indexes into memory, allow additional writes.
	// Closed: no additional writes allowed, read-only.
	file.Seek(-FOOTER_LENGTH, 2)
	footer := binary.ReadBytes(file, FOOTER_LENGTH)

	if string(footer) == string(MAGIC_FOOTER) {
		return readonly(path)
	} else {
		return read(path)
	}
}
