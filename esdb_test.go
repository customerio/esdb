package esdb

import (
	"encoding/csv"
	"math/rand"
	"os"
	"reflect"
	"testing"
)

func fetchBlockIndex(db *Db, id []byte, index string) []string {
	found := make([]string, 0)

	block := db.Find(id)

	if block != nil {
		block.ScanIndex(index, func(event *Event) bool {
			found = append(found, string(event.Data))
			return true
		})
	}

	return found
}

var events Events

func createDb() *Db {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.esdb")

	w, err := New("tmp/test.esdb")
	if err != nil {
		println(err.Error())
	}
	populate(w)
	err = w.Write()
	if err != nil {
		println(err.Error())
	}

	db, err := Open("tmp/test.esdb")
	if err != nil {
		println(err.Error())
	}

	return db
}

func populate(w *Writer) {
	events = Events{
		newEvent(2, []byte("1")),
		newEvent(3, []byte("2")),
		newEvent(1, []byte("3")),
		newEvent(1, []byte("4")),
		newEvent(1, []byte("5")),
		newEvent(2, []byte("6")),
	}

	w.Add([]byte("a"), events[0].Timestamp, events[0].Data, "g", []string{"", "i1", "i2"})
	w.Add([]byte("a"), events[1].Timestamp, events[1].Data, "h", []string{"", "i2"})
	w.Add([]byte("a"), events[2].Timestamp, events[2].Data, "i", []string{"", "i1"})
	w.Add([]byte("b"), events[3].Timestamp, events[3].Data, "g", []string{"", "i1"})
	w.Add([]byte("b"), events[4].Timestamp, events[4].Data, "h", []string{"", "i1"})
	w.Add([]byte("b"), events[5].Timestamp, events[5].Data, "i", []string{"", "i1", "i2"})
}

func TestBlockIndexes(t *testing.T) {
	db := createDb()

	var tests = []struct {
		id    string
		index string
		want  []string
	}{
		{"a", "", []string{"3", "1", "2"}},
		{"a", "i1", []string{"3", "1"}},
		{"a", "i2", []string{"1", "2"}},
		{"b", "", []string{"4", "5", "6"}},
		{"b", "i1", []string{"4", "5", "6"}},
		{"b", "i2", []string{"6"}},
		{"b", "i3", []string{}},
		{"c", "", []string{}},
	}

	for i, test := range tests {
		found := fetchBlockIndex(db, []byte(test.id), test.index)

		if !reflect.DeepEqual(test.want, found) {
			t.Errorf("Case #%v: wanted: %v, found: %v", i, test.want, found)
		}
	}
}

func fetch100RowColumns(file string, index int) []string {
	f, _ := os.Open("testdata/million_visits.csv")
	c := csv.NewReader(f)

	columns := make([]string, 100)

	for i := 0; i < 100; i++ {
		row, err := c.Read()
		if err != nil {
			panic(err)
		}

		// third column is city, which
		// is an index in our test file.
		columns[i] = row[index]
	}

	return columns
}

func BenchmarkMillionEventDbScanSingle(b *testing.B) {
	// csv index 1 is host, which is the grouping in our test file.
	hosts := fetch100RowColumns("testdata/million_visits.csv", 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		host := hosts[rand.Intn(100)]
		db.Find([]byte("visit")).Scan(host, func(e *Event) bool {
			return false
		})
	}
}

func BenchmarkMillionEventDbScanIndexSingle(b *testing.B) {
	// csv index 2 is city, which is an index in our test file.
	cities := fetch100RowColumns("testdata/million_visits.csv", 2)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		city := cities[rand.Intn(100)]
		db.Find([]byte("visit")).ScanIndex(city, func(e *Event) bool {
			return false
		})
	}
}

func BenchmarkMillionEventDbScan500(b *testing.B) {
	// csv index 1 is host, which is the grouping in our test file.
	hosts := fetch100RowColumns("testdata/million_visits.csv", 1)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		count := 0

		host := hosts[rand.Intn(100)]
		db.Find([]byte("visit")).Scan(host, func(e *Event) bool {
			count += 1
			return count < 500
		})
	}
}

func BenchmarkMillionEventDbScanIndex500(b *testing.B) {
	// csv index 2 is city, which is an index in our test file.
	cities := fetch100RowColumns("testdata/million_visits.csv", 2)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, err := Open("testdata/million_visits.esdb")
		if err != nil {
			panic(err)
		}

		count := 0

		city := cities[rand.Intn(100)]
		db.Find([]byte("visit")).ScanIndex(city, func(e *Event) bool {
			count += 1
			return count < 500
		})
	}
}
