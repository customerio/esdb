package esdb

import (
	"io"
	"sort"

	"github.com/customerio/esdb/blocks"
)

// writes block/offset locations for all events
// associated with the given index to the file
// in timestamp descending order.
// Marks the event with which block it's located in,
// as well as the offset within the block.
func writeIndexBlocks(i *index, out io.Writer) {
	sort.Stable(sort.Reverse(i.evs))

	writer := blocks.NewWriter(out, 4096)

	for _, event := range i.evs {
		// Each entry in the index is
		// the block the event is located in,
		// and the offset within the block.
		writeInt64(writer, event.block)
		writeInt16(writer, event.offset)
	}

	// Mark the end of the grouping's events with an empty event.
	writer.Write([]byte{0})

	writer.Flush()

	i.length += writer.Written
}
