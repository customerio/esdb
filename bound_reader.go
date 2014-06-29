package esdb

import (
	"io"
)

// boundReaders implement io.Reader and io.Seeker, while
// bounding both reads and seeks to the range defined
// by the provided start and stop offsets.
type boundReader struct {
	reader  io.ReadSeeker
	start   int64
	stop    int64
	current int64
	whence  int
}

func newBoundReader(reader io.ReadSeeker, start, stop int64) io.ReadSeeker {
	var whence int

	if start < 0 {
		whence = 2
		reader.Seek(start, 2)
	} else {
		whence = 0
		reader.Seek(start, 0)
	}

	return &boundReader{
		reader,
		start,
		stop,
		start,
		whence,
	}
}

func (r *boundReader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	r.current += int64(n)

	if r.current > r.stop {
		n -= int(r.current - r.stop)

		r.current = r.stop

		if n < 0 {
			return 0, io.EOF
		}
	}

	return
}

func (r *boundReader) Seek(offset int64, whence int) (n int64, err error) {
	if whence == 0 {
		offset = r.start + offset
	} else if whence == 1 {
		offset = r.current + offset
	} else if whence == 2 {
		offset = r.stop + offset
	}

	if offset < r.start {
		offset = r.start
	}

	r.current = offset

	return r.reader.Seek(offset, r.whence)
}
