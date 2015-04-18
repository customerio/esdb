package main

import (
	"github.com/customerio/esdb/cluster"

	"flag"
	"fmt"
	"log"
	"os"
)

var node = flag.String("n", "localhost:4001", "url for node")
var stop = flag.Uint64("stop", 0, "commit to clear until")

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

	if *stop == 0 {
		flag.Usage()
		log.Fatal("stop commit is required")
	}

	dbpath := flag.Arg(0)

	client := cluster.NewLocalClient("http://"+*node, 1)

	meta, err := client.StreamsMetadata()
	if err != nil {
		log.Fatal(err)
	}

	err = cluster.Clear(dbpath, *stop, meta.Closed)
	if err != nil {
		log.Fatal(err)
	}
}
