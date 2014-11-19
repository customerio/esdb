package cluster

import (
	"github.com/jrallison/raft"
)

type CompressCommand struct {
	Start uint64 `json:"start"`
	Stop  uint64 `json:"stop"`
}

func NewCompressCommand(start, stop uint64) *CompressCommand {
	return &CompressCommand{start, stop}
}

func (c *CompressCommand) CommandName() string {
	return "compress"
}

func (c *CompressCommand) Apply(context raft.Context) (interface{}, error) {
	server := context.Server()
	db := server.Context().(*DB)

	db.Compress(c.Start, c.Stop)

	return new(interface{}), nil
}
