package esdb

import (
	"encoding/csv"
	"math/rand"
	"os"
	"reflect"
	"testing"
)

func fetchSpaceIndex(db *Db, id []byte, index, value string) []string {
	found := make([]string, 0)

	space := db.Find(id)

	if space != nil {
		space.ScanIndex(index, value, func(event *Event) bool {
			found = append(found, string(event.Data))
			return true
		})
	}

	return found
}

var evs events

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
	evs = events{
		newEvent([]byte("1"), 2),
		newEvent([]byte("2"), 3),
		newEvent([]byte("3"), 1),
		newEvent([]byte("4"), 3),
		newEvent([]byte("5"), 1),
		newEvent([]byte("6"), 2),
	}

	w.Add([]byte("a"), evs[0].Data, evs[0].Timestamp, "g", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("a"), evs[1].Data, evs[1].Timestamp, "h", map[string]string{"ts": "", "i": "i2"})
	w.Add([]byte("a"), evs[2].Data, evs[2].Timestamp, "i", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("b"), evs[3].Data, evs[3].Timestamp, "g", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("b"), evs[4].Data, evs[4].Timestamp, "h", map[string]string{"ts": "", "i": "i1"})
	w.Add([]byte("b"), evs[5].Data, evs[5].Timestamp, "i", map[string]string{"ts": "", "i": "i1"})
}

func TestSpaceIndexes(t *testing.T) {
	db := createDb()

	var tests = []struct {
		id    string
		index string
		value string
		want  []string
	}{
		{"a", "ts", "", []string{"2", "1", "3"}},
		{"a", "i", "i1", []string{"1", "3"}},
		{"a", "i", "i2", []string{"2"}},
		{"b", "ts", "", []string{"4", "6", "5"}},
		{"b", "i", "i1", []string{"4", "6", "5"}},
		{"b", "i", "i2", []string{}},
		{"b", "i", "i3", []string{}},
		{"c", "ts", "", []string{}},
	}

	for i, test := range tests {
		found := fetchSpaceIndex(db, []byte(test.id), test.index, test.value)

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
		db.Find([]byte("visit")).ScanIndex("city", city, func(e *Event) bool {
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
		db.Find([]byte("visit")).ScanIndex("city", city, func(e *Event) bool {
			count += 1
			return count < 500
		})
	}
}
