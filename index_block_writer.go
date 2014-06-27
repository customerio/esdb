package esdb

import (
	"io"
	"sort"

	"github.com/customerio/esdb/blocks"
)

func writeIndexBlocks(i *index, out io.Writer) int {
	sort.Stable(i.evs)

	writer := blocks.NewWriter(out, 4096)

	writer.Write([]byte{0})

	for _, event := range i.evs {
		buf := newWriteBuffer([]byte{})

		buf.PushUint64(event.block)
		buf.PushUint16(event.offset)

		writer.Write(buf.Bytes())
	}

	writer.Write([]byte{0})
	writer.Flush()

	i.length += writer.Written

	return writer.Blocks
}
