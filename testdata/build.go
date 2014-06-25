package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/customerio/esdb"
)

type visit struct {
	EventType string `json:"type"`
	Host      string `json:"host"`
	City      string `json:"city"`
	Visitor   string `json:"visitor"`
	Timestamp int    `json:"timestamp"`
	data      []byte
}

func main() {
	for _, file := range []string{
		"ten_thousand_visits.csv",
		"hundred_thousand_visits.csv",
		"million_visits.csv",
	} {

		esdbFile := strings.Replace(file, ".csv", ".esdb", 1)
		jsonFile := strings.Replace(file, ".csv", ".json", 1)
		os.Remove(esdbFile)

		f, err := os.Open(file)
		if err != nil {
			panic(err)
		}

		start := time.Now()
		fmt.Println("reading", file, "...")

		rows, _ := csv.NewReader(f).ReadAll()

		visits := make([]visit, len(rows))

		for i, row := range rows {
			timestamp, _ := strconv.Atoi(row[4])

			visits[i] = visit{
				row[0],
				row[1],
				row[2],
				row[3],
				timestamp,
				nil,
			}

			visits[i].data, _ = json.Marshal(visits[i])
		}

		fmt.Println("done:", time.Since(start))

		start = time.Now()
		fmt.Println("writing", esdbFile, "...")

		w, err := esdb.New(esdbFile)
		if err != nil {
			panic(err)
		}

		js, err := os.Create(jsonFile)
		if err != nil {
			panic(err)
		}

		for _, e := range visits {
			w.Add([]byte(e.EventType), e.Timestamp, e.data, e.Host, []string{e.Visitor, e.City})
			js.Write(append(e.data, byte('\n')))
		}

		err = w.Write()
		if err != nil {
			panic(err)
		}

		js.Close()

		fmt.Println("done:", time.Since(start))
	}
}
