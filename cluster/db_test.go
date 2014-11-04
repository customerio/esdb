package cluster

import (
	"os"
	"testing"
)

func createDb() *DB {
	os.RemoveAll("tmp")
	os.MkdirAll("tmp", 0755)

	return NewDb("tmp")
}

func TestDir(t *testing.T) {
	db := createDb()

	if db.dir != "tmp" {
		t.Errorf("DB path is incorrect. Want: tmp, Got: %v", db.dir)
	}
}

func TestMostRecent(t *testing.T) {
	db := createDb()

	db.setCurrent(1)

	db.Write(2, []byte("hello"), map[string]string{}, 1415118695524660)
	db.Write(3, []byte("hello"), map[string]string{}, 1415118695524662)
	db.Write(4, []byte("hello"), map[string]string{}, 1415118695524661)

	if db.MostRecent != 1415118695524662 {
		t.Errorf("Incorrect most recent. Want: %v, Got: %v", 1415118695524662, db.MostRecent)
	}
}
