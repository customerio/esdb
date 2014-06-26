package esdb

import (
	"io"
	"sort"
)

func writeIndexBlocks(i *index, out io.Writer) (blocks []int) {
	sort.Stable(sort.Reverse(i.evs))

	buf := newWriteBuffer([]byte{})
	blocks = make([]int, 0)

	var write = func(limit int) {
		for buf.Len() > limit {
			blocks = append(blocks, i.length)
			n, _ := out.Write(buf.Next(4096))
			i.length += n
		}
	}

	for _, event := range i.evs {
		buf.PushUint64(event.block)
		buf.PushUint16(event.offset)
		write(4096)
	}

	write(0)

	return
}
