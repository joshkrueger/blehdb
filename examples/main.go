package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	goji "goji.io"
	"goji.io/pat"

	"github.com/joshkrueger/blehdb"
)

const (
	DefaultHTTPAddr = ":9000"
	DefaultRaftAddr = ":12000"
)

var db *blehdb.Server

var httpAddr string
var raftAddr string
var joinAddr string

func init() {
	flag.StringVar(&httpAddr, "addr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set the Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set the join address (optional)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	fmt.Println("Starting BlehDB Test Server...")

	var err error
	storageDir := "somedir"
	config := blehdb.DefaultConfig()

	config.StorageDir = storageDir
	config.RaftBind = ":11000"
	config.RPCBind = ":12000"

	db, err = blehdb.NewServer(config)
	if err != nil {
		panic(err)
	}

	mux := goji.NewMux()
	mux.HandleFunc(pat.Get("/status"), handleStatus)
	mux.HandleFunc(pat.Get("/data/:bucket/:key"), handleGetKey)
	mux.HandleFunc(pat.Post("/data/:bucket/:key"), handleSetKey)
	mux.HandleFunc(pat.Delete("/data/:bucket/:key"), handleDeleteKey)
	mux.HandleFunc(pat.Post("/data/:bucket"), handleCreateBucket)
	mux.HandleFunc(pat.Delete("/data/:bucket"), handleDeleteBucket)
	mux.HandleFunc(pat.Get("/data"), handleListBuckets)

	go func() {
		err := http.ListenAndServe(":9000", mux)
		if err != nil {
			log.Fatalf("HTTP Serve: %s", err)
		}
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Println("exiting!")
}
