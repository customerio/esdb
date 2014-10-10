package cluster

import (
	"github.com/goraft/raft"
)

const (
	ROTATE_THRESHOLD = 100
)

type RotateCommand struct {
	Timestamp int64 `json:"timestamp"`
}

func NewRotateCommand(timestamp int64) *RotateCommand {
	return &RotateCommand{
		Timestamp: timestamp,
	}
}

func (c *RotateCommand) CommandName() string {
	return "rotate"
}

func (c *RotateCommand) Apply(server raft.Server) (interface{}, error) {
	db := server.Context().(*DB)
	return new(interface{}), db.Rotate(c.Timestamp)
}
