package cluster

import (
	"os"
	"testing"
)

func createDb() *DB {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/events.stream")

	return NewDb("tmp")
}

func TestDir(t *testing.T) {
	db := createDb()

	if db.dir != "tmp" {
		t.Errorf("DB path is incorrect. Want: tmp, Got: %v", db.dir)
	}
}
