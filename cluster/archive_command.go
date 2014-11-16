package cluster

import (
	"github.com/jrallison/raft"
)

type ArchiveCommand struct {
	Start uint64 `json:"start"`
	Stop  uint64 `json:"stop"`
}

func NewArchiveCommand(start, stop uint64) *ArchiveCommand {
	return &ArchiveCommand{start, stop}
}

func (c *ArchiveCommand) CommandName() string {
	return "archive"
}

func (c *ArchiveCommand) Apply(context raft.Context) (interface{}, error) {
	server := context.Server()
	db := server.Context().(*DB)

	db.Archive(c.Start, c.Stop)

	return new(interface{}), nil
}
