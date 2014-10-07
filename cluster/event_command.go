package cluster

import (
	"github.com/goraft/raft"
)

type EventCommand struct {
	Body    []byte            `json:"body"`
	Indexes map[string]string `json:"indexes"`
}

func NewEventCommand(body []byte, indexes map[string]string) *EventCommand {
	return &EventCommand{
		Body:    body,
		Indexes: indexes,
	}
}

func (c *EventCommand) CommandName() string {
	return "event"
}

func (c *EventCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(*DB)
	return "", db.Write(c.Body, c.Indexes)
}
