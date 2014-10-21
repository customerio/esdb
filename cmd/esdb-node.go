package main

import (
	"github.com/customerio/esdb/cluster"
	"github.com/jrallison/raft"

	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

var trace = flag.Bool("trace", false, "Raft trace debugging")
var debug = flag.Bool("debug", false, "Raft debugging")
var host = flag.String("h", "localhost", "hostname")
var port = flag.Int("p", 4001, "port")
var join = flag.String("join", "", "host:port of node in a cluster to join")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	log.SetFlags(0)

	flag.Parse()

	if *trace {
		raft.SetLogLevel(raft.Trace)
		log.Print("Raft trace debugging enabled.")
	} else if *debug {
		raft.SetLogLevel(raft.Debug)
		log.Print("Raft debugging enabled.")
	}

	rand.Seed(time.Now().UnixNano())

	// Set the data directory.
	if flag.NArg() == 0 {
		flag.Usage()
		log.Fatal("Data path argument required")
	}

	path := flag.Arg(0)

	if err := os.MkdirAll(path, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}

	log.SetFlags(log.LstdFlags)

	n := cluster.NewNode(path, *host, *port)

	log.Fatal(n.Start(*join))
}
