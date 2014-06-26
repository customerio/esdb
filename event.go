package esdb

type events []*Event

func (e events) Len() int           { return len(e) }
func (e events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Event struct {
	Data      []byte
	Timestamp int
	block     int
	offset    int
}

func newEvent(data []byte, timestamp int) *Event {
	return &Event{data, timestamp, 0, 0}
}

func pullEvent(buf *buffer) (e *Event) {
	size := buf.PullUvarint()
	data := buf.Pull(size)

	if len(data) > 0 {
		e = &Event{Data: data}
	}

	return
}

func (e *Event) push(buf *writeBuffer) {
	buf.PushUvarint(len(e.Data))
	buf.Push(e.Data)
}
