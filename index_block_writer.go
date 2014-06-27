package esdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"

	"github.com/customerio/esdb/blocks"
)

func writeIndexBlocks(i *index, out io.Writer) int {
	sort.Stable(sort.Reverse(i.evs))

	writer := blocks.NewWriter(out, 4096)

	for _, event := range i.evs {
		buf := new(bytes.Buffer)

		binary.Write(buf, binary.LittleEndian, uint64(event.block))
		binary.Write(buf, binary.LittleEndian, uint16(event.offset))

		writer.Write(buf.Bytes())
	}

	writer.Write([]byte{0})
	writer.Flush()

	i.length += writer.Written

	return writer.Blocks
}
