package esdb

import (
    "testing"
    "reflect"
    "os"
    "encoding/csv"
    "encoding/json"
    "sort"
    "strconv"
)


func TestInspectSpaces(t *testing.T){

    db, err := Open("testdata/million_visits.esdb")
    if err != nil {
        panic(err)
    }

    found_spaces := []*Space{}

    for _, space := range db.InspectSpaces(){
        found_spaces = append(found_spaces, space)
    }

    spaces := []*Space{db.Find([]byte("visit"))}

    if !reflect.DeepEqual(spaces, found_spaces){
        t.Errorf("Inspected spaces didn't match actual. wanted: %v, found: %v", spaces, found_spaces)
    }

}


type DumpList []Event

func (d DumpList) Len() int           { return len(d) }
func (d DumpList) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d DumpList) Less(i, j int) bool {

    if d[i].Timestamp != d[j].Timestamp {
        return d[i].Timestamp > d[j].Timestamp
    }else{
        return string(d[i].Data) > string(d[j].Data)
    }
}

type dump_visit struct {
    EventType string `json:"type"`
    Host      string `json:"host"`
    City      string `json:"city"`
    Visitor   string `json:"visitor"`
    Timestamp int    `json:"timestamp"`
    data      []byte
}


func TestDumpAll(t *testing.T){

    // Load our comparison set
    f, _ := os.Open("testdata/ten_thousand_visits.csv")
    c := csv.NewReader(f)

    expected := DumpList{}

    for {
        row, err := c.Read()
        if err != nil {
            break
        }
        t, _ := strconv.Atoi(row[4])
        jsondata, _ := json.Marshal(dump_visit{
            row[0],
            row[1],
            row[2],
            row[3],
            t,
            nil,
        })
        expected = append(expected, Event{Timestamp: t, Data: jsondata})
    }

    sort.Sort(expected)

    db, err := Open("testdata/ten_thousand_visits.esdb")
    if err != nil {
        panic(err)
    }

    dumped := DumpList{}
    for _, events := range db.DumpAll(){
        for _, event := range events {
            dumped = append(dumped, *event)
        }
    }

    sort.Sort(dumped)

    if len(expected) != 10000 {
        t.Errorf("Event count mismatch, expected: %v, got %v", len(expected), len(dumped))
    }

    for i, event := range expected {
        if dumped[i].Timestamp != event.Timestamp || !reflect.DeepEqual(dumped[i].Data, event.Data){
            t.Errorf("Bad dump data, got: %v/%s, expected: %v/%s", dumped[i].Timestamp, dumped[i].Data, event.Timestamp, event.Data)
        }
    }
}
