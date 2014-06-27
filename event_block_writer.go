package esdb

import (
	"bytes"
	"io"
	"sort"

	"github.com/customerio/esdb/blocks"
)

func writeEventBlocks(i *index, out io.Writer) (count int) {
	sort.Stable(sort.Reverse(i.evs))

	writer := blocks.NewWriter(out, 4096)

	for _, event := range i.evs {
		buf := new(bytes.Buffer)

		event.block = i.offset + writer.Written
		event.offset = writer.Buffered()

		event.push(buf)
		writer.Write(buf.Bytes())
	}

	writer.Write([]byte{0})
	writer.Flush()
	i.length += writer.Written

	return writer.Blocks
}
