package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	BlockRestartInterval = 16
	BlockSize            = 4096
	BlockTrailerLen      = 5
)

var table = crc32.MakeTable(crc32.Castagnoli)

type indexEntry struct {
	bh     blockHandle
	keyLen int
}

type Writer struct {
	writer    io.Writer
	offset    uint64
	prevKey   []byte
	pendingBH blockHandle

	indexKeys    []byte
	indexEntries []indexEntry

	buf      bytes.Buffer
	nEntries int
	restarts []uint32
	tmp      [50]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer:   w,
		prevKey:  make([]byte, 0, 256),
		restarts: make([]uint32, 0, 256),
	}
}

func (w *Writer) Set(key, value []byte) error {
	if bytes.Compare(w.prevKey, key) >= 0 {
		return fmt.Errorf("Set called with unordered keys: %q, %q", w.prevKey, key)
	}

	w.flushPendingBH(key)

	w.append(key, value, w.nEntries%BlockRestartInterval == 0)

	// If the estimated block size is sufficiently large, finish the current block.
	if w.buf.Len()+4*(len(w.restarts)+1) >= BlockSize {
		bh, err := w.finishBlock()
		if err != nil {
			return err
		}

		w.pendingBH = bh
	}
	return nil
}

func (w *Writer) Close() (err error) {
	// Finish the last data block, or force an empty data block if there
	// aren't any data blocks at all.
	w.flushPendingBH(nil)
	if w.nEntries > 0 || len(w.indexEntries) == 0 {
		bh, err := w.finishBlock()
		if err != nil {
			return err
		}
		w.pendingBH = bh
		w.flushPendingBH(nil)
	}

	// Write the (empty) metaindex block.
	metaindexBlockHandle, err := w.finishBlock()
	if err != nil {
		return err
	}

	// Write the index block.
	// writer.append uses w.tmp[:3*binary.MaxVarintLen64].
	i0, tmp := 0, w.tmp[3*binary.MaxVarintLen64:5*binary.MaxVarintLen64]
	for _, ie := range w.indexEntries {
		n := encodeBlockHandle(tmp, ie.bh)
		i1 := i0 + ie.keyLen
		w.append(w.indexKeys[i0:i1], tmp[:n], true)
		i0 = i1
	}
	indexBlockHandle, err := w.finishBlock()
	if err != nil {
		return err
	}

	// Write the table footer.
	footer := w.tmp[:FOOTER_SIZE]
	for i := range footer {
		footer[i] = 0
	}
	n := encodeBlockHandle(footer, metaindexBlockHandle)
	encodeBlockHandle(footer[n:], indexBlockHandle)
	copy(footer[FOOTER_SIZE-len(MAGIC):], MAGIC)
	if _, err := w.writer.Write(footer); err != nil {
		return err
	}

	return nil
}

func (w *Writer) flushPendingBH(key []byte) {
	if w.pendingBH.length == 0 {
		return
	}

	n0 := len(w.indexKeys)
	w.indexKeys = appendSeparator(w.indexKeys, w.prevKey, key)
	n1 := len(w.indexKeys)
	w.indexEntries = append(w.indexEntries, indexEntry{w.pendingBH, n1 - n0})
	w.pendingBH = blockHandle{}
}

// append appends a key/value pair, which may also be a restart point.
func (w *Writer) append(key, value []byte, restart bool) {
	nShared := 0
	if restart {
		w.restarts = append(w.restarts, uint32(w.buf.Len()))
	} else {
		nShared = sharedPrefixLen(w.prevKey, key)
	}
	w.prevKey = append(w.prevKey[:0], key...)
	w.nEntries++
	n := binary.PutUvarint(w.tmp[0:], uint64(nShared))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(key)-nShared))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(value)))
	w.buf.Write(w.tmp[:n])
	w.buf.Write(key[nShared:])
	w.buf.Write(value)
}

func (w *Writer) finishBlock() (blockHandle, error) {
	if w.nEntries == 0 {
		w.restarts = w.restarts[:1]
		w.restarts[0] = 0
	}
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf.Write(tmp4)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf.Write(tmp4)

	b := w.buf.Bytes()
	w.tmp[0] = 0

	checksum := crc32.Update(0, table, b)
	checksum = crc32.Update(checksum, table, w.tmp[:1])
	checksum = uint32(checksum>>15|checksum<<17) + 0xa282ead8
	binary.LittleEndian.PutUint32(w.tmp[1:5], checksum)

	if _, err := w.writer.Write(b); err != nil {
		return blockHandle{}, err
	}
	if _, err := w.writer.Write(w.tmp[:5]); err != nil {
		return blockHandle{}, err
	}
	bh := blockHandle{int64(w.offset), int64(len(b))}
	w.offset += uint64(len(b)) + BlockTrailerLen

	w.buf.Reset()
	w.nEntries = 0
	w.restarts = w.restarts[:0]
	return bh, nil
}

func appendSeparator(dst, a, b []byte) []byte {
	i, n := sharedPrefixLen(a, b), len(dst)
	dst = append(dst, a...)
	if len(b) > 0 {
		if i == len(a) {
			return dst
		}
		if i == len(b) {
			panic("a < b is a precondition, but b is a prefix of a")
		}
		if a[i] == 0xff || a[i]+1 >= b[i] {
			return dst
		}
	}
	i += n
	for ; i < len(dst); i++ {
		if dst[i] != 0xff {
			dst[i]++
			return dst[:i+1]
		}
	}
	return dst
}

func sharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}
