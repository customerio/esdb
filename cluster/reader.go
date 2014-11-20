package cluster

import (
	"github.com/customerio/esdb/stream"

	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Reader struct {
	dir     string
	closed  []uint64
	current uint64
	peers   []string
	stream  stream.Stream
	streams map[uint64]stream.Stream
	mutexes map[uint64]*sync.Mutex
}

func NewReader(path string) *Reader {
	return &Reader{
		dir:     path,
		closed:  make([]uint64, 0),
		streams: make(map[uint64]stream.Stream),
		mutexes: make(map[uint64]*sync.Mutex),
	}
}

func (r *Reader) Scan(name, value, continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	commit, offset := r.parseContinuation(continuation, true)

	for !stopped && commit > 0 {
		s, err := r.retrieveStream(commit, true)
		if err != nil {
			return "", err
		}

		err = s.ScanIndex(name, value, offset, func(e *stream.Event) bool {
			offset = e.Next(name, value)
			stopped = !scanner(e)
			return !stopped
		})

		if err != nil {
			return "", err
		}

		if !stopped {
			commit = r.Prev(commit)
			offset = 0
		}
	}

	if stopped && offset == 0 {
		commit = r.Prev(commit)
	}

	return r.buildContinuation(commit, offset), nil
}

func (r *Reader) Iterate(continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	commit, offset := r.parseContinuation(continuation, false)

	for !stopped && commit > 0 {
		s, err := r.retrieveStream(commit, true)
		if err != nil {
			return "", err
		}

		offset, err = s.Iterate(offset, func(e *stream.Event) bool {
			stopped = !scanner(e)
			return !stopped
		})

		if err != nil {
			return "", err
		}

		if !stopped {
			commit = r.Next(commit)
			offset = 0
		}
	}

	return r.buildContinuation(commit, offset), nil
}

func (r *Reader) Prev(commit uint64) uint64 {
	var result uint64

	for _, c := range r.closed {
		if c < commit && c > result {
			result = c
		}
	}

	return result
}

func (r *Reader) Next(commit uint64) uint64 {
	var result uint64

	result = math.MaxUint64

	for _, c := range r.closed {
		if c > commit && c < result {
			result = c
		}
	}

	if r.current > commit && r.current < result {
		result = r.current
	}

	if result == math.MaxUint64 {
		result = 0
	}

	return result
}

func (r *Reader) Update(peers []string, closed []uint64, current uint64, stream stream.Stream) {
	r.peers = peers

	if len(closed) < len(r.closed) {
		// If we shrank, we compressed, to be safe,
		// let's reopen all the things.
		for _, closed := range r.closed {
			r.mutex(closed).Lock()
			defer r.mutex(closed).Unlock()

			r.forgetStream(closed)
		}
	}

	r.current = current
	r.stream = stream
	r.closed = closed
}

func (r *Reader) retrieveStream(commit uint64, fetchMissing bool) (stream.Stream, error) {
	if commit == r.current {
		return r.stream, nil
	}

	r.mutex(commit).Lock()
	defer r.mutex(commit).Unlock()

	if r.streams[commit] == nil {
		var err error
		(func() {
			if r.streams[commit] == nil {
				var s stream.Stream
				var missing bool

				s, err = stream.Open(r.Path(commit))

				if err != nil && strings.Contains(err.Error(), "no such file or directory") {
					missing = true
				}

				if s != nil && !s.Closed() {
					println("found open stream:", commit)
					missing = true
					s = nil
				}

				if missing && fetchMissing {
					s, err = RecoverStream(r.peers, r.dir, fmt.Sprintf("events.%024v.stream", commit))
				}

				if err == nil {
					r.streams[commit] = s
				}
			}
		})()

		if err != nil {
			return nil, err
		}
	}

	return r.streams[commit], nil
}

func (r *Reader) forgetStream(commit uint64) {
	if r.streams[commit] != nil {
		r.streams[commit].Close()
	}

	delete(r.streams, commit)
}

func (r *Reader) mutex(commit uint64) *sync.Mutex {
	if r.mutexes[commit] == nil {
		r.mutexes[commit] = &sync.Mutex{}
	}

	return r.mutexes[commit]
}

func (r *Reader) Path(commit uint64) string {
	return filepath.Join(r.dir, fmt.Sprintf("events.%024v.stream", commit))
}

func (r *Reader) compressedpath(commit uint64) string {
	return filepath.Join(r.dir, fmt.Sprintf("events.%024v.tmpstream", commit))
}

func (r *Reader) parseContinuation(continuation string, reverse bool) (uint64, int64) {
	commit := r.current

	if !reverse && len(r.closed) > 0 {
		commit = r.closed[0]
	}

	var offset int64

	if continuation != "" {
		parts := strings.SplitN(continuation, ":", 2)

		if len(parts) == 2 {
			commit, _ = strconv.ParseUint(parts[0], 10, 64)
			offset, _ = strconv.ParseInt(parts[1], 10, 64)
		}
	}

	return commit, offset
}

func (r *Reader) buildContinuation(commit uint64, offset int64) string {
	if commit > 0 {
		return fmt.Sprint(commit, ":", offset)
	} else {
		return ""
	}
}
