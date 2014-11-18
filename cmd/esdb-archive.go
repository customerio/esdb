package main

import (
	"github.com/customerio/esdb/cluster"

	"flag"
	"fmt"
	"log"
	"os"
)

var node = flag.String("n", "localhost:4001", "url for node")
var start = flag.Uint64("start", 0, "commit to start compressing")
var stop = flag.Uint64("stop", 0, "commit to stop compressing")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	log.SetFlags(0)

	flag.Parse()

	if *start == 0 || *stop == 0 {
		flag.Usage()
		log.Fatal("start and stop commits are required")
	}

	client := cluster.NewClient("http://" + *node)

	if err := client.Archive(*start, *stop); err != nil {
		log.Fatal(err)
	}
}
