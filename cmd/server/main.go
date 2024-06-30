package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/mayank-02/surfstore/pkg/surfstore"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./server -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddrs*): BlockStore addresses (IP:port) the MetaStore should be initialized with. (Note: if service_type = both, then use the address of the server that you're starting)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}
	if len(args) >= 1 {
		blockStoreAddrs = args
	}

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(io.Discard)
	}

	log.Fatal(StartServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func StartServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	// Create listener
	lis, err := net.Listen("tcp", hostAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", hostAddr, err)
	}

	// Create gRPC server and register services
	grpcServer := grpc.NewServer()

	switch serviceType {
	case "block":
		surfstore.RegisterBlockStoreServer(grpcServer, surfstore.NewBlockStore())

	case "meta":
		surfstore.RegisterMetaStoreServer(grpcServer, surfstore.NewMetaStore(blockStoreAddrs))

	case "both":
		surfstore.RegisterBlockStoreServer(grpcServer, surfstore.NewBlockStore())
		surfstore.RegisterMetaStoreServer(grpcServer, surfstore.NewMetaStore(blockStoreAddrs))

	default:
		return fmt.Errorf("invalid service type: %s", serviceType)
	}
	log.Printf("Registered %s store(s) with gRPC server", serviceType)

	// Accept connections on the listener
	log.Printf("Starting %s server(s) on %s...\n", serviceType, hostAddr)
	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC server: %v", err)
	}

	return nil
}
