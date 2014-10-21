package cluster

import (
	"github.com/goraft/raft"

	"log"
)

const (
	ROTATE_THRESHOLD = 4
)

type EventCommand struct {
	Body      []byte            `json:"body"`
	Indexes   map[string]string `json:"indexes"`
	Timestamp int64             `json:"timestamp"`
}

func NewEventCommand(body []byte, indexes map[string]string, timestamp int64) *EventCommand {
	return &EventCommand{
		Body:      body,
		Indexes:   indexes,
		Timestamp: timestamp,
	}
}

func (c *EventCommand) CommandName() string {
	return "event"
}

func (c *EventCommand) Apply(context raft.Context) (interface{}, error) {
	server := context.Server()
	db := server.Context().(*DB)

	err := db.Write(c.Body, c.Indexes)

	if err == nil && db.offset%ROTATE_THRESHOLD == 0 {
		println("rotating", db.offset)
		err = db.Rotate()
		if err != nil {
			log.Fatal(err)
		}
	}

	return new(interface{}), err
}
