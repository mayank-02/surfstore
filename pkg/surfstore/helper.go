package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return filepath.Join(baseDir, fileDir)
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `CREATE TABLE IF NOT EXISTS indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetadataMap map[string]*FileMetaData, baseDir string) error {
	// Remove metastore file if it exists
	metaFile := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(metaFile); err == nil { // file exists
		e := os.Remove(metaFile)
		if e != nil {
			log.Fatalf("Failed to remove the existing metastore file present at %s", metaFile)
		}
	}

	// Create a new metastore file
	db, err := sql.Open("sqlite3", metaFile)
	if err != nil {
		log.Fatalf("Failed to open %s file.", metaFile)
	}
	defer db.Close()

	// Create a table in the database
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatalf("Failed to create table in %s file.", metaFile)
	}
	statement.Exec()

	// Insert tuples into the table
	for _, fileMetadata := range fileMetadataMap {
		for index, blockHash := range fileMetadata.BlockHashList {
			statement, err := db.Prepare(insertTuple)
			if err != nil {
				log.Fatalf("Failed to insert tuple into %s file.", metaFile)
			}
			statement.Exec(fileMetadata.Filename, fileMetadata.Version, index, blockHash)
		}
	}

	return nil
}

/*
	Reading Local Metadata File Related
*/

const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes;`

const getTuplesByFileName string = `SELECT version, hashIndex, hashValue FROM indexes WHERE fileName = ? ORDER BY hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	fileMetaMap = make(map[string]*FileMetaData)

	// Get the absolute path of the metadata file
	metaFilePath, err := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Check if the metadata file exists
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}

	// Open the metadata file
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer db.Close()

	// Get all distinct file names
	distinctFileNames, err := db.Query(getDistinctFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to get distinct file names from %s file: %w", metaFilePath, err)
	}
	defer distinctFileNames.Close()

	// Iterate over all file names
	for distinctFileNames.Next() {
		var fileName string
		distinctFileNames.Scan(&fileName)

		// Get all tuples for the current file name
		hashBlocks, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			return nil, fmt.Errorf("failed to get tuples for file name %s from %s file: %w", fileName, metaFilePath, err)
		}
		defer hashBlocks.Close()

		// Create a new file metadata object
		fileMeta := &FileMetaData{
			Filename:      fileName,
			BlockHashList: make([]string, 0),
			Version:       0,
		}

		// Iterate over all tuples for the current file name
		for hashBlocks.Next() {
			var version int
			var hashIndex int
			var hashValue string
			hashBlocks.Scan(&version, &hashIndex, &hashValue)
			fileMeta.Version = int32(version)
			fileMeta.BlockHashList = append(fileMeta.BlockHashList, hashValue)
		}

		// Add the file metadata object to the map
		fileMetaMap[fileName] = fileMeta
	}

	if err := distinctFileNames.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate on distinct filenames: %w", err)
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
