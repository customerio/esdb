package esdb

import (
	"bytes"
	"testing"
)

func TestBlockWriterImmutability(t *testing.T) {
	writer := newBlock(new(bytes.Buffer), []byte("a"))
	writer.write()

	err := writer.add(1, []byte("1"), "b", []string{"", "i2"})

	if err == nil || err.Error() != "Cannot add to block. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written block")
	}
}
