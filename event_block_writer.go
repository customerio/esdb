package esdb

import (
	"io"
	"sort"
)

func writeEventBlocks(i *index, out io.Writer) (blocks int) {
	sort.Stable(sort.Reverse(i.evs))

	buf := newWriteBuffer([]byte{})

	var write = func(limit int) {
		for buf.Len() > limit {
			n, _ := out.Write(buf.Next(4096))
			i.length += n
			blocks += 1
		}
	}

	for _, event := range i.evs {
		event.block = i.length
		event.offset = buf.Len()
		event.push(buf)
		write(4096)
	}

	buf.Push([]byte{0})

	write(0)

	return
}
