package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

// GetResponsibleServer returns the address of the BlockStore server
// that holds the block with the given block hash.
func (c ConsistentHashRing) GetResponsibleServer(blockHash string) string {
	// Create a sorted list of server hashes
	serverHashes := make([]string, 0)
	for serverHash := range c.ServerMap {
		serverHashes = append(serverHashes, serverHash)
	}
	sort.Strings(serverHashes)

	// Find the first server hash that is greater than the block hash. Modulo the
	// length of servers in case the block hash is greater than all server hashes.
	n := sort.SearchStrings(serverHashes, blockHash) % len(serverHashes)
	return c.ServerMap[serverHashes[n]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	ring := ConsistentHashRing{ServerMap: make(map[string]string)}
	for _, addr := range serverAddrs {
		prefixedAddr := "blockstore" + addr
		ring.ServerMap[ring.Hash(prefixedAddr)] = addr
	}
	return &ring
}
