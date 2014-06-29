package esdb

import (
	"bytes"
	"io"
	"sort"

	"github.com/customerio/esdb/blocks"
)

func writeIndexBlocks(i *index, out io.Writer) int {
	sort.Stable(sort.Reverse(i.evs))

	writer := blocks.NewWriter(out, 4096)

	for _, event := range i.evs {
		buf := new(bytes.Buffer)

		writeInt64(buf, event.block)
		writeInt16(buf, event.offset)
		writer.Write(buf.Bytes())
	}

	writer.Write([]byte{0})
	writer.Flush()

	i.length += writer.Written

	return writer.Blocks
}
