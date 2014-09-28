package stream

import (
	"os"
)

type Scanner func(*Event) bool

type Stream interface {
	Write(data []byte, indexes []string) (int, error)
	ScanIndex(index string, scanner Scanner) error
	Iterate(scanner Scanner) error
	Closed() bool
	Close() error
}

func Open(path string) (Stream, error) {
	_, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// 2 states a stream file can be in:
	// Open: repopulate indexes into memory, allow additional writes.
	// Closed: no additional writes allowed, read-only.
	closed := false

	if closed {
		return Readonly(path)
	} else {
		return Read(path)
	}
}
