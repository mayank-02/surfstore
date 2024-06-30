package surfstore

import (
	"context"
	"slices"

	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

// Retrieves a block indexed by hash value h
func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block, found := bs.BlockMap[blockHash.Hash]
	if !found {
		return nil, status.Errorf(codes.NotFound, "block corresponding to hash %s not found", blockHash.Hash)
	}
	return block, nil
}

// Stores block b in the key-value store, indexed by hash value h
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// Generate the hash of the block
	hash := GetBlockHashString(block.BlockData)

	// Check for hash collision
	if _, found := bs.BlockMap[hash]; found {
		if slices.Compare(bs.BlockMap[hash].BlockData, block.BlockData) != 0 {
			return nil, status.Errorf(codes.AlreadyExists, "blocks with same hash but different data encountered")
		}
		return &Success{Flag: true}, nil
	}

	// Store the block in the key-value store
	bs.BlockMap[hash] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes, returns a list containing the hashes that are NOT stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	missingBlocks := &BlockHashes{}
	for _, blockHash := range blockHashesIn.Hashes {
		if _, found := bs.BlockMap[blockHash]; !found {
			missingBlocks.Hashes = append(missingBlocks.Hashes, blockHash)
		}
	}
	return missingBlocks, nil
}

// Returns a list containing all block hashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := &BlockHashes{}
	for key := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, key)
	}
	return blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
