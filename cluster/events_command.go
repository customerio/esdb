package cluster

import (
	"github.com/jrallison/raft"

	"log"
)

type EventsCommand struct {
	Bodies    [][]byte            `json:"bodies"`
	Indexes   []map[string]string `json:"indexes"`
	Timestamp int64               `json:"timestamp"`
}

func NewEventsCommand(bodies [][]byte, indexes []map[string]string, timestamp int64) *EventsCommand {
	return &EventsCommand{
		Bodies:    bodies,
		Indexes:   indexes,
		Timestamp: timestamp,
	}
}

func (c *EventsCommand) CommandName() string {
	return "events"
}

func (c *EventsCommand) Apply(context raft.Context) (interface{}, error) {
	server := context.Server()
	db := server.Context().(*DB)

	index := context.CurrentIndex()

	err := db.WriteAll(index, c.Bodies, c.Indexes, c.Timestamp)

	if err == nil && db.Offset() > db.RotateThreshold {
		err = db.Rotate(index, context.CurrentTerm())
		if err != nil {
			log.Fatal(err)
		}
	}

	return new(interface{}), err
}
