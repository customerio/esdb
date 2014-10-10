package cluster

import (
	"github.com/goraft/raft"
)

type EventCommand struct {
	Body      []byte            `json:"body"`
	Indexes   map[string]string `json:"indexes"`
	Timestamp int64             `json:"timestamp"`
}

type EventCommandResponse struct {
	Rotate bool
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

func (c *EventCommand) Apply(server raft.Server) (interface{}, error) {
	res := EventCommandResponse{}

	db := server.Context().(*DB)
	err := db.Write(c.Body, c.Indexes)

	if err == nil && db.Offset() > ROTATE_THRESHOLD {
		res.Rotate = true
	}

	return res, err
}
