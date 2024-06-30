package surfstore

import (
	"errors"
	"io"
	"log"
	"os"
	"slices"
	"strings"
)

var ErrFileVersion = errors.New("file has an outdated version")

func ClientSync(client RPCClient) {
	// Read the local metadata file
	log.Printf("Loading local index from %v/%v\n", client.BaseDir, DEFAULT_META_FILENAME)
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatalf("Failed to load local index: %v\n", err)
	}

	// Traverse the base directory and update the local metadata
	filesInBaseDir, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatalf("Failed to read directory: %v\n", err)
	}

	// Iterate over all files in the base directory
	// Handles the case where a file is created or modified locally
	for _, file := range filesInBaseDir {
		if file.IsDir() || file.Name() == DEFAULT_META_FILENAME || strings.Contains(file.Name(), ",") {
			log.Printf("Skipping %s\n", file.Name())
			continue
		}

		f, err := os.Open(ConcatPath(client.BaseDir, file.Name()))
		if err != nil {
			log.Fatalf("Failed to open file: %v\n", err)
		}
		defer f.Close()

		fileName := file.Name()
		fileHashList := GetHashListForFile(f, client.BlockSize)
		if _, found := localIndex[fileName]; !found {
			log.Printf("File %v: Created after last sync, saved to local index\n", fileName)
			localIndex[fileName] = &FileMetaData{Filename: fileName, BlockHashList: fileHashList, Version: 1}
		} else {
			if !slices.Equal(fileHashList, localIndex[fileName].BlockHashList) {
				localIndex[fileName].BlockHashList = fileHashList
				localIndex[fileName].Version++
				log.Printf("File %v: Modified after last sync, new version = %v\n", fileName, localIndex[fileName].Version)
			} else {
				log.Printf("File %v: Not modified after last sync\n", fileName)
			}
		}
	}

	// Check for files that were deleted locally
	// Convert file names in base directory to a map for faster lookups
	filesNamesMap := make(map[string]struct{})
	for _, file := range filesInBaseDir {
		filesNamesMap[file.Name()] = struct{}{}
	}

	for fileName, fileMetaData := range localIndex {
		if _, found := filesNamesMap[fileName]; !found {
			if fileMetaData.BlockHashList[0] != TOMBSTONE_HASHVALUE {
				// File was deleted locally, so we increment the version and set the block hash list to TOMBSTONE_HASHVALUE
				log.Printf("File %v: Deleted locally after last sync\n", fileName)
				fileMetaData.Version++
				fileMetaData.BlockHashList = []string{TOMBSTONE_HASHVALUE}
			} else {
				log.Printf("File %v: Already deleted locally after last sync\n", fileName)
			}
		}
	}

	// Get the remote metadata
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatalf("Failed to get remote index: %v\n", err)
	}

	// Compare the local and remote metadata
	filesOnlyInLocal := findUniqueToFirst(localIndex, remoteIndex)
	filesOnlyInRemote := findUniqueToFirst(remoteIndex, localIndex)
	commonFiles := findCommonFiles(localIndex, remoteIndex)

	// File exists only in remote, so download the file from remote
	for fileName, fileMetaData := range filesOnlyInRemote {
		// CASE A: File was created remotely and local needs to be synced.
		DownloadFile(client, fileName, fileMetaData)
		localIndex[fileName] = fileMetaData
		log.Printf("File %v: Synced!\n", fileName)
	}

	// File exists only in local, so upload the file to remote
	for fileName, fileMetaData := range filesOnlyInLocal {
		// CASE B: File was created locally and does not exist in remote
		err := UploadFile(client, fileName, fileMetaData)
		if errors.Is(err, ErrFileVersion) {
			// If the version is outdated, download the file from the remote
			log.Printf("WARNING: File %v: Remote rejected upload, version is outdated. Remote metadata changed in a single invocation of the client.\n", fileName)
		} else if err != nil {
			log.Fatalf("Failed to upload file %v: %v\n", fileName, err)
		}
		log.Printf("File %v: Synced!\n", fileName)
	}

	// File exists in both local and remote
	for _, fileName := range commonFiles {

		// Grab the local and remote metadata for the file
		localFileMetaData := localIndex[fileName]
		remoteFileMetaData := remoteIndex[fileName]
		log.Printf("File %v: Local Version = %v, Remote Version = %v\n", fileName, localFileMetaData.Version, remoteFileMetaData.Version)

		switch {
		case localFileMetaData.Version == remoteFileMetaData.Version:
			// CASE C: Local file is in sync with remote
			// CASE D: Remote file was updated and local needs to be overridden.
			if !slices.Equal(localFileMetaData.BlockHashList, remoteFileMetaData.BlockHashList) {
				log.Printf("File %v: Local and Remote Blocks do not match, downloading from remote...\n", fileName)
				DownloadFile(client, fileName, remoteFileMetaData)
				localIndex[fileName] = remoteFileMetaData
			}
		case localFileMetaData.Version == remoteFileMetaData.Version+1:
			// CASE E: Local file has changes which need to be synced to remote.
			log.Printf("File %v: Unsynced changes in local, uploading to remote...\n", fileName)
			err := UploadFile(client, fileName, localFileMetaData)
			if errors.Is(err, ErrFileVersion) {
				log.Printf("WARNING: File %v: Remote rejected upload, version is outdated. Remote metadata changed in a single invocation of the client.\n", fileName)
			} else if err != nil {
				log.Fatalf("Failed to upload file %v: %v\n", fileName, err)
			}
		case localFileMetaData.Version < remoteFileMetaData.Version:
			// CASE F: Remote file has changes which need to be synced to local.
			log.Printf("File %v: Version is outdated, downloading from remote...\n", fileName)
			DownloadFile(client, fileName, remoteFileMetaData)
			localIndex[fileName] = remoteFileMetaData
		default:
			log.Fatalf("File %v: Invalid versioning state. Local Version = %v, Remote Version = %v\n", fileName, localFileMetaData.Version, remoteFileMetaData.Version)
			continue
		}

		log.Printf("File %v: Synced!\n", fileName)
	}

	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatalf("Failed to get remote index: %v\n", err)
	}

	WriteMetaFile(localIndex, client.BaseDir)
}

func GetHashListForFile(f *os.File, blockSize int) []string {
	// Special case for empty files
	stat, err := f.Stat()
	if err != nil {
		log.Fatalf("Failed to get file stats: %v\n", err)
	}
	if stat.Size() == 0 {
		return []string{EMPTYFILE_HASHVALUE}
	}
	log.Printf("File size: %v\n", stat.Size())

	// Read the file in block size'd blocks and hash each block
	hashList := make([]string, 0)
	buffer := make([]byte, blockSize)
	for {
		n, err := io.ReadFull(f, buffer)
		if err == io.EOF {
			break
		}
		hash := GetBlockHashString(buffer[:n])
		hashList = append(hashList, hash)
	}

	return hashList
}

func findUniqueToFirst(A, B map[string]*FileMetaData) map[string]*FileMetaData {
	unique := make(map[string]*FileMetaData)
	for key, value := range A {
		if _, found := B[key]; !found {
			unique[key] = value
		}
	}
	return unique
}

func findCommonFiles(A, B map[string]*FileMetaData) []string {
	common := make([]string, 0)
	for key := range A {
		if _, found := B[key]; found {
			common = append(common, key)
		}
	}
	return common
}

func DownloadFile(client RPCClient, fileName string, fileMetaData *FileMetaData) {
	if fileMetaData.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		// File was deleted remotely, so we remove it locally
		if err := os.Remove(ConcatPath(client.BaseDir, fileName)); err != nil {
			if os.IsNotExist(err) {
				log.Printf("File %v: Deleted remotely, does not exist locally\n", fileName)
				return
			}
			log.Fatalf("File %v: Deleted remotely, but failed to remove it: %v\n", fileName, err)
		}
		log.Printf("File %v: Deleted remotely, removed locally\n", fileName)
		return
	}

	// Create the file
	f, err := os.Create(ConcatPath(client.BaseDir, fileName))
	if err != nil {
		log.Fatalf("Failed to create file: %v\n", err)
	}
	defer f.Close()

	// Empty file, so we don't need to download anything
	if fileMetaData.BlockHashList[0] == EMPTYFILE_HASHVALUE {
		log.Printf("File %v: Empty file downloaded\n", fileName)
		return
	}

	// Get the block store address for each block
	serverToBlockMap := make(map[string][]string)
	err = client.GetBlockStoreMap(fileMetaData.BlockHashList, &serverToBlockMap)
	if err != nil {
		log.Fatalf("Failed to get block store address: %v", err)
	}
	blockToServerMap := reverseServerToBlockMap(serverToBlockMap)

	// Download each block and write it to the file
	for _, blockHash := range fileMetaData.BlockHashList {
		// Get the block store address

		block := &Block{}
		err = client.GetBlock(blockHash, blockToServerMap[blockHash], block)
		if err != nil {
			log.Fatalf("Failed to download block %v of file %v: %v", blockHash, fileName, err)
		}

		_, err = f.Write(block.BlockData)
		if err != nil {
			log.Fatalf("Failed to write block %v of file %v: %v", blockHash, fileName, err)
		}
	}

	// Flush the file to disk
	f.Sync()

	log.Printf("File %v: Downloaded %v blocks\n", fileName, len(fileMetaData.BlockHashList))
}

func UploadFile(client RPCClient, fileName string, fileMetaData *FileMetaData) error {
	if fileMetaData.BlockHashList[0] == TOMBSTONE_HASHVALUE || fileMetaData.BlockHashList[0] == EMPTYFILE_HASHVALUE {
		return updateFileMetadata(client, fileMetaData)
	}

	// Open the file
	f, err := os.Open(ConcatPath(client.BaseDir, fileName))
	if err != nil {
		log.Fatal("Failed to open file: ", err)
	}
	defer f.Close()

	// Get the block store address for each block
	serverToBlockMap := make(map[string][]string)
	err = client.GetBlockStoreMap(fileMetaData.BlockHashList, &serverToBlockMap)
	if err != nil {
		log.Fatalf("Failed to get block store address: %v", err)
	}
	blockToServerMap := reverseServerToBlockMap(serverToBlockMap)

	// Read the file in blocks and upload each block
	blocksRead := 0
	buffer := make([]byte, client.BlockSize)
	for {
		n, err := io.ReadFull(f, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Fatalf("Failed to read block %v of file %v: %v", blocksRead, fileName, err)
		}

		// Check if we have reached the end of the file
		if n == 0 {
			break
		}

		// Upload the block
		block := &Block{BlockData: buffer[:n], BlockSize: int32(n)}
		success, err := client.PutBlock(block, blockToServerMap[fileMetaData.BlockHashList[blocksRead]])
		if !success || err != nil {
			log.Fatalf("Failed to upload block %v of file %v: %v", blocksRead, fileName, err)
		}
		blocksRead++

		// Break if we have read less than client.BlockSize, indicating EOF
		if n < client.BlockSize {
			break
		}
	}

	log.Printf("File %v: Uploaded %v blocks\n", fileName, blocksRead)

	return updateFileMetadata(client, fileMetaData)
}

func updateFileMetadata(client RPCClient, fileMetaData *FileMetaData) error {
	version, err := client.UpdateFile(fileMetaData)
	if err != nil {
		log.Fatalf("File %v: Failed to update metadata because %v", fileMetaData.Filename, err)
	}
	if version == -1 {
		log.Printf("WARNING: File %v: Remote rejected upload, version is outdated. Remote metadata changed in a single invocation of the client.\n", fileMetaData.Filename)
		return ErrFileVersion
	}

	log.Printf("File %v: Version %v metadata updated in MetaStore\n", fileMetaData.Filename, version)

	return nil
}

func reverseServerToBlockMap(serverToBlockMap map[string][]string) map[string]string {
	blockToServerMap := make(map[string]string)
	for server, blocks := range serverToBlockMap {
		for _, block := range blocks {
			blockToServerMap[block] = server
		}
	}
	return blockToServerMap
}
