package esdb

import (
	"os"
	"testing"
)

func TestWriterImmutability(t *testing.T) {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.esdb")
	w, _ := New("tmp/test.esdb")
	w.Finalize()

	err := w.Add([]byte("b"), 1, []byte("1"), "i1", []string{"", "i2"})

	if err == nil || err.Error() != "Cannot add to database. We're immutable and this one has already been written." {
		t.Errorf("Failed to throw error when adding events to a written database")
	}
}

func TestDisallowOverwrittingExistingDb(t *testing.T) {
	createDb()

	_, err := New("tmp/test.esdb")

	if err == nil {
		t.Errorf("Didn't raise error when attempting to write to existing database")
	}
}
