package surfstore

import (
	context "context"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	return nil
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string) (bool, error) {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false, err
	}
	defer conn.Close()

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = c.PutBlock(ctx, block)
	if err != nil {
		return false, err
	}

	return true, err
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	missingBlocks, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		return err
	}

	*blockHashesOut = missingBlocks.Hashes
	return nil
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	hashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}

	*blockHashes = hashes.Hashes
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			return err
		}

		*serverFileInfoMap = fileInfoMap.FileInfoMap
		return nil
	}

	return ErrMissingLeader
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData) (int32, error) {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return 0, err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		version, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			return 0, err
		}

		return version.Version, nil
	}

	return 0, ErrMissingLeader
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		b, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			return err
		}

		for server, hashes := range b.BlockStoreMap {
			(*blockStoreMap)[server] = hashes.Hashes
		}

		return nil
	}

	return ErrMissingLeader
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		bsAddr, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), ErrServerCrashed.Error()) || strings.Contains(err.Error(), ErrNotLeader.Error()) {
				continue
			}
			return err
		}

		*blockStoreAddrs = bsAddr.BlockStoreAddrs
		return nil
	}

	return ErrMissingLeader
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
