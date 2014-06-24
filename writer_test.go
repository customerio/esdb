package esdb

import (
	"encoding/csv"
	"encoding/json"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

type visit struct {
	EventType string `json:"type"`
	Host      string `json:"host"`
	City      string `json:"city"`
	Visitor   string `json:"visitor"`
	Timestamp int    `json:"timestamp"`
	data      []byte
}

func readVisits(path string, count int) []visit {
	os.RemoveAll("tmp")
	os.MkdirAll("tmp", 0755)

	f, _ := os.Open(path)
	w := csv.NewReader(f)
	rows, _ := w.ReadAll()

	visits := make([]visit, 0, count)

	for i, row := range rows {
		timestamp, _ := strconv.Atoi(row[4])

		visits = append(visits, visit{
			row[0],
			row[1],
			row[2],
			row[3],
			timestamp,
			nil,
		})

		visits[i].data, _ = json.Marshal(visits[i])
	}

	return visits
}

func TestWriterImmutability(t *testing.T) {
	os.MkdirAll("tmp", 0755)
	os.Remove("tmp/test.esdb")
	w, _ := New("tmp/test.esdb")
	w.Write()

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

func BenchmarkWriteTenThousandEvents(b *testing.B) {
	visits := readVisits("testdata/ten_thousand_visits.csv", 10000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, _ := New("tmp/bench" + strconv.Itoa(i) + ".esdb")

		for _, e := range visits {
			w.Add([]byte(e.EventType), e.Timestamp, e.data, e.Host, []string{e.Visitor, e.City})
		}

		err := w.Write()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWriteTenThousandEventsNoGrouping(b *testing.B) {
	visits := readVisits("testdata/ten_thousand_visits.csv", 10000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, _ := New("tmp/bench" + strconv.Itoa(i) + ".esdb")

		for _, e := range visits {
			w.Add([]byte(e.EventType), e.Timestamp, e.data, "", []string{e.Host, e.Visitor, e.City})
		}

		err := w.Write()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWriteTenThousandEventsMuchBlocks(b *testing.B) {
	visits := readVisits("testdata/ten_thousand_visits.csv", 10000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, _ := New("tmp/bench" + strconv.Itoa(i) + ".esdb")

		for _, e := range visits {
			w.Add([]byte(strconv.Itoa(rand.Intn(1000))), e.Timestamp, e.data, e.Host, []string{e.Host, e.Visitor, e.City})
		}

		err := w.Write()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWriteHundredThousandEvents(b *testing.B) {
	visits := readVisits("testdata/hundred_thousand_visits.csv", 100000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, _ := New("tmp/bench" + strconv.Itoa(i) + ".esdb")

		for _, e := range visits {
			w.Add([]byte(e.EventType), e.Timestamp, e.data, e.Host, []string{e.Visitor, e.City})
		}

		err := w.Write()
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkWriteMillionEvents(b *testing.B) {
	visits := readVisits("testdata/million_visits.csv", 1000000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w, _ := New("tmp/bench" + strconv.Itoa(i) + ".esdb")

		for _, e := range visits {
			w.Add([]byte(e.EventType), e.Timestamp, e.data, e.Host, []string{e.Visitor, e.City})
		}

		err := w.Write()
		if err != nil {
			panic(err)
		}
	}
}
