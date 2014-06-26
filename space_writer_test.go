package esdb

import (
	"bytes"
	"testing"
)

func TestSpaceWriterImmutability(t *testing.T) {
	writer := newSpace(new(bytes.Buffer), []byte("a"))
	writer.write()

	err := writer.add([]byte("1"), 1, "b", nil)

	if err == nil || err.Error() != "Cannot add to space. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written space")
	}
}
