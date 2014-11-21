package main

import (
	"github.com/customerio/esdb/cluster"

	"flag"
	"fmt"
	"log"
	"os"
)

var node = flag.String("n", "localhost:4001", "url for node")
var start = flag.Uint64("start", 0, "commit to start merging")
var stop = flag.Uint64("stop", 0, "commit to stop merging")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	log.SetFlags(0)

	flag.Parse()

	// Set the data directory.
	if flag.NArg() == 0 {
		flag.Usage()
		log.Fatal("Data path argument required")
	}

	if *start == 0 || *stop == 0 {
		flag.Usage()
		log.Fatal("start and stop commits are required")
	}

	dbpath := flag.Arg(0)

	client := cluster.NewLocalClient("http://"+*node, 1)

	meta, err := client.StreamsMetadata()
	if err != nil {
		log.Fatal(err)
	}

	err = cluster.Merge(dbpath, *start, *stop, meta.Closed)
	if err != nil {
		log.Fatal(err)
	}
}
