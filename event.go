package esdb

type Events []*Event

func (e Events) Len() int           { return len(e) }
func (e Events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e Events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Event struct {
	Data      []byte
	Timestamp int
	next      map[string]int
	prev      map[string]int
}
