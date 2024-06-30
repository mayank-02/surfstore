package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/mayank-02/surfstore/pkg/surfstore"
)

// Arguments
const ARG_COUNT int = 2

// Usage strings
const USAGE_STRING = "./client -d -f configFile.json baseDir blockSize"

const DEBUG_NAME = "d"
const DEBUG_USAGE = "Output log statements"

const CONFIG_NAME = "f config_file.json"
const CONFIG_USAGE = "Path to config file that specifies addresses for all Raft nodes"

const BASEDIR_NAME = "baseDir"
const BASEDIR_USAGE = "Base directory of the client"

const BLOCK_NAME = "blockSize"
const BLOCK_USAGE = "Size of the blocks used to fragment files"

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		fmt.Fprintf(w, "  -%s: %v\n", DEBUG_NAME, DEBUG_USAGE)
		fmt.Fprintf(w, "  -%s: %v\n", CONFIG_NAME, CONFIG_USAGE)
		fmt.Fprintf(w, "  %s: %v\n", BASEDIR_NAME, BASEDIR_USAGE)
		fmt.Fprintf(w, "  %s: %v\n", BLOCK_NAME, BLOCK_USAGE)
	}

	// Parse command-line arguments and flags
	debug := flag.Bool("d", false, DEBUG_USAGE)
	configFile := flag.String("f", "", "(required) Config file")
	flag.Parse()

	// Use tail arguments to hold non-flag arguments
	args := flag.Args()

	if len(args) != ARG_COUNT {
		flag.Usage()
		os.Exit(EX_USAGE)
	}
	addrs := surfstore.LoadRaftConfigFile(*configFile)

	// Parse arguments
	baseDir := args[0]
	blockSize, err := strconv.Atoi(args[1])
	if blockSize <= 0 || err != nil {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	log.Println("Client syncing with ", addrs, baseDir, blockSize)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}

	// Create a new SurfstoreRPCClient
	rpcClient := surfstore.NewSurfstoreRPCClient(addrs.RaftAddrs, baseDir, blockSize)

	// Synchronize the changes b/w the client and the MetaStore
	surfstore.ClientSync(rpcClient)
}
