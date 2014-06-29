package esdb

import (
	"io"
	"sort"

	"github.com/customerio/esdb/blocks"
)

// writes all events associated with the given
// grouping to the file in timestamp descending order.
// Marks the event with which block it's located in,
// as well as the offset within the block.
func writeEventBlocks(i *index, out io.Writer) {
	sort.Stable(sort.Reverse(i.evs))

	writer := blocks.NewWriter(out, 4096)

	for _, event := range i.evs {
		// mark event with the current location in the file.
		event.block = i.offset + int64(writer.Written)
		event.offset = writer.Buffered()

		// push the encoded event onto the buffer.
		event.push(writer)
	}

	// Mark the end of the grouping's events with an empty event.
	writer.Write([]byte{0})

	writer.Flush()

	i.length += int64(writer.Written)
}
