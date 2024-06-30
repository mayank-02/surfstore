package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Raft configuration
type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

// Load the Raft configuration file
func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	config, err := os.Open(filename)
	if err != nil {
		log.Fatal("failed to open config file:", err)
	}
	defer config.Close()

	decoder := json.NewDecoder(bufio.NewReader(config))
	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal("failed to decode config file:", err)
	}

	return
}

// Initialize a new Raft server
func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	conns := make([]*grpc.ClientConn, len(config.RaftAddrs))
	for i, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns[i] = conn
	}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &sync.RWMutex{},
		term:              0,
		metaStore:         NewMetaStore(config.BlockAddrs),
		log:               make([]*UpdateOperation, 0),
		id:                id,
		commitIndex:       -1,
		lastApplied:       -1,
		nextIndex:         make(map[int64]int64),
		matchIndex:        make(map[int64]int64),
		unreachableFrom:   make(map[int64]bool),
		grpcServer:        grpc.NewServer(),
		rpcConns:          conns,
		clusterMembers:    config.RaftAddrs,
		raftStateMutex:    &sync.RWMutex{},
	}

	return &server, nil
}

// Start the Raft server
func ServeRaftServer(s *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(s.grpcServer, s)

	listener, err := net.Listen("tcp", s.clusterMembers[s.id])
	if err != nil {
		return err
	}

	log.Printf("[Server %d] Starting Raft server on %s\n", s.id, s.clusterMembers[s.id])

	return s.grpcServer.Serve(listener)
}
