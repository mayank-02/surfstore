package surfstore

import (
	"context"
	"errors"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

// Returns a mapping of the files stored in the SurfStore cloud service,
// including the version, filename, and hashlist.
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

// Updates the FileInfo values associated with a file stored in the cloud.
// This method replaces the hash list for the file with the provided hash
// list only if the new version number is exactly one greater than the
// current version number. Otherwise, it returns version=-1 to the client
// telling them that the version they are trying to store is not right
// (likely too old).
func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	if fileMetaData == nil {
		return &Version{Version: -1}, errors.New("fileMetaData is nil")
	}

	fileName := fileMetaData.Filename
	if _, found := m.FileMetaMap[fileName]; !found {
		// If the file does not exist, the version must be 1
		if fileMetaData.Version != 1 {
			return &Version{Version: -1}, fmt.Errorf("file %s does not exist, but version is not 1", fileName)
		}
		m.FileMetaMap[fileName] = fileMetaData
		return &Version{Version: 1}, nil
	} else {
		// If the file exists, the version must be exactly current version + 1
		if fileMetaData.Version != m.FileMetaMap[fileName].Version+1 {
			return &Version{Version: -1}, nil
		}
		m.FileMetaMap[fileName] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	}
}

// GetBlockStoreMap determines the responsible block server for each block hash
// provided in the input and returns a mapping of block server addresses to
// their respective block hashes.
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	if blockHashesIn == nil || blockHashesIn.Hashes == nil {
		return nil, errors.New("blockHashesIn or blockHashesIn.Hashes is nil")
	}

	blockStoreMap := &BlockStoreMap{BlockStoreMap: map[string]*BlockHashes{}}
	for _, blockHash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, found := blockStoreMap.BlockStoreMap[server]; !found {
			blockStoreMap.BlockStoreMap[server] = &BlockHashes{Hashes: []string{}}
		}
		blockStoreMap.BlockStoreMap[server].Hashes = append(blockStoreMap.BlockStoreMap[server].Hashes, blockHash)
	}
	return blockStoreMap, nil
}

// GetBlockStoreAddrs returns all the BlockStore addresses.
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
