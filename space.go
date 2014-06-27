package esdb

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/customerio/esdb/blocks"
	"github.com/customerio/esdb/sst"
)

type Scanner func(*Event) bool

type Space struct {
	Id []byte

	buf    *buffer
	offset uint64
	index  *sst.Reader
}

func openSpace(reader io.ReadSeeker, id []byte, offset, length uint64) *Space {
	if st, err := findSpaceIndex(reader, offset, length); err == nil {
		return &Space{
			Id:     id,
			index:  st,
			buf:    newBuffer(blocks.NewReader(reader, 4096), offset, offset+length, 4096),
			offset: offset,
		}
	}

	return nil
}

func (s *Space) Scan(grouping string, scanner Scanner) {
	if offset := s.findGroupingOffset(grouping); offset > 0 {
		s.buf.Move(s.offset+offset, 4096)

		for event := pullEvent(s.buf); event != nil; {
			if !scanner(event) {
				return
			}

			event = pullEvent(s.buf)
		}
	}
}

func (s *Space) ScanIndex(name, value string, scanner Scanner) {
	if buf := s.findIndex(name, value); buf != nil {
		buf.Pull(1) // Gets rid of nil marker

		block := buf.PullUint64()
		offset := uint64(buf.PullUint16())
		s.buf.Move(s.offset+block+offset, 0)

		for event := pullEvent(s.buf); block+offset > 0 && event != nil; {
			if !scanner(event) {
				return
			}

			block = buf.PullUint64()
			offset = uint64(buf.PullUint16())
			s.buf.Move(s.offset+block+offset, 0)
			event = pullEvent(s.buf)
		}
	}
}

func (s *Space) RevScanIndex(name, value string, scanner Scanner) {
	if buf := s.findIndex(name, value); buf != nil {
		buf.Pull(1) // Gets rid of nil marker

		offset := uint64(buf.PopUint16())
		block := buf.PopUint64()
		s.buf.Move(s.offset+block+offset, 0)

		for event := pullEvent(s.buf); block+offset > 0 && event != nil; {
			if !scanner(event) {
				return
			}

			offset = uint64(buf.PopUint16())
			block = buf.PopUint64()
			s.buf.Move(s.offset+block+offset, 0)
			event = pullEvent(s.buf)
		}
	}
}

func (s *Space) findGroupingOffset(name string) uint64 {
	if val, err := s.index.Get([]byte("g" + name)); err == nil {
		buf := newByteBuffer(val)
		return buf.PullUvarint64()
	}

	return 0
}

func (s *Space) findIndex(name, value string) *buffer {
	if val, err := s.index.Get([]byte("i" + name + ":" + value)); err == nil {
		buf := newByteBuffer(val)

		offset := buf.PullUvarint64()
		length := buf.PullUvarint64()

		return newBuffer(s.buf.reader, s.offset+offset, s.offset+offset+length, 4096)
	}

	return nil
}

func findSpaceIndex(r io.ReadSeeker, offset, length uint64) (*sst.Reader, error) {
	r.Seek(int64(offset+length-4), 0)

	var indexLength int32
	binary.Read(r, binary.LittleEndian, &indexLength)

	r.Seek(int64(offset+length-4-uint64(indexLength)), 0)
	index := make([]byte, indexLength)
	r.Read(index)

	return sst.NewReader(bytes.NewReader(index), int64(indexLength))
}
