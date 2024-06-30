package surftest

import (
	"log"
	"os"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// A creates and syncs with a file. B creates and syncs with same file. A syncs again.
func TestSyncTwoClientsSameFileLeaderFailure(t *testing.T) {
	t.Log("Client1 syncs with file1. Client2 syncs with file1 (different content). Client1 syncs again.")
	cfgPath := "./config_files/3nodes.json"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	// Clients add different files
	file1 := "client1.txt"
	file2 := "client1.txt"
	err := worker1.AddFile(file1)
	if err != nil {
		t.FailNow()
	}
	err = worker2.AddFile(file2)
	if err != nil {
		t.FailNow()
	}
	err = worker2.UpdateFile(file2, "update text")
	if err != nil {
		t.FailNow()
	}

	// Client1 syncs
	t.Log("[Orchestrator] Client1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client2 syncs
	t.Log("[Orchestrator] Client2 syncs")
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	//client1 syncs
	t.Log("[Orchestrator] Client1 syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	workingDir, _ := os.Getwd()

	// Check client1
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}
	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1 == nil || fileMeta1[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client1 metadata.")
	}
	c, e := SameFile(workingDir+"/test0/client1.txt", SRC_PATH+"/client1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("file1 should not change at client1")
	}

	// Check client2
	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}
	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2 == nil || fileMeta2[file1].Version != 1 {
		t.Fatalf("Wrong version for file1 in client2 metadata.")
	}
	c, e = SameFile(workingDir+"/test1/client1.txt", SRC_PATH+"/client1.txt")
	if e != nil {
		t.Fatalf("Could not read files in client base dirs.")
	}
	if !c {
		t.Fatalf("wrong file2 contents at client2")
	}
}

func TestRemoteDeleteSync(t *testing.T) {
	cfgPath := "./config_files/3nodes.json"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	// Copy the same file to both clients
	file := "hello.txt"
	err := worker1.AddFile(file)
	if err != nil {
		t.Fatalf("Failed to add file for worker1: %v", err)
	}

	// Sync both clients
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client1 initial sync failed: %v", err)
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client2 initial sync failed: %v", err)
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Client 2 deletes the file
	err = worker2.DeleteFile(file)
	if err != nil {
		t.Fatalf("Client2 failed to delete file: %v", err)
	}

	// Sync both clients again
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client2 sync after delete failed: %v", err)
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client1 sync after delete failed: %v", err)
	}
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	if err != nil {
		t.Fatalf("Failed getting internal state: %v", err)
	}

	// Verify file metadata and existence on both clients
	workingDir, _ := os.Getwd()
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}
	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	log.Println(fileMeta1)
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file].Version != 2 {
		t.Fatalf("Wrong version for file in client1 metadata, got %d", fileMeta1[file].Version)
	}
	if fileMeta1[file].BlockHashList[0] != TOMBSTONE_HASH {
		t.Fatalf("Expected tombstone hash for deleted file in client1 metadata.")
	}

	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}
	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2[file].Version != 2 {
		t.Fatalf("Wrong version for file in client2 metadata.")
	}
	if fileMeta2[file].BlockHashList[0] != TOMBSTONE_HASH {
		t.Fatalf("Expected tombstone hash for deleted file in client2 metadata.")
	}

	// Verify the file does not exist in both clients' directories
	if _, err := os.Stat(worker1.DirectoryName + "/" + file); err == nil {
		t.Fatalf("File should not exist in client1's directory")
	}
	if _, err := os.Stat(worker2.DirectoryName + "/" + file); err == nil {
		t.Fatalf("File should not exist in client2's directory")
	}
}

func TestDeleteAndMultipleSync(t *testing.T) {
	t.Log("Test delete file and multiple syncs")
	cfgPath := "./config_files/3nodes.json"
	test := InitTest(cfgPath)
	defer EndTest(test)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker1 := InitDirectoryWorker("test0", SRC_PATH)
	worker2 := InitDirectoryWorker("test1", SRC_PATH)
	defer worker1.CleanUp()
	defer worker2.CleanUp()

	// Copy the file to client1
	file := "hello.txt"
	err := worker1.AddFile(file)
	if err != nil {
		t.Fatalf("Failed to add file for worker1: %v", err)
	}

	// Sync client1 and client2
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client1 initial sync failed: %v", err)
	}
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client2 initial sync failed: %v", err)
	}

	// Delete the file in client1 and sync again
	err = worker1.DeleteFile(file)
	if err != nil {
		t.Fatalf("Client1 failed to delete file: %v", err)
	}

	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client1 sync after delete failed: %v", err)
	}
	err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
	if err != nil {
		t.Fatalf("Client2 sync after client1 delete failed: %v", err)
	}

	// Verify file metadata and existence on both clients
	workingDir, _ := os.Getwd()
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client1")
	}
	fileMeta1, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file].Version != 2 {
		t.Fatalf("Wrong version for file in client1 metadata.")
	}
	if fileMeta1[file].BlockHashList[0] != TOMBSTONE_HASH {
		t.Fatalf("Expected tombstone hash for deleted file in client1 metadata.")
	}

	_, err = os.Stat(workingDir + "/test1/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client2")
	}
	fileMeta2, err := LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2[file].Version != 2 {
		t.Fatalf("Wrong version for file in client2 metadata.")
	}
	if fileMeta2[file].BlockHashList[0] != TOMBSTONE_HASH {
		t.Fatalf("Expected tombstone hash for deleted file in client2 metadata.")
	}

	// Verify the file does not exist in both clients' directories
	if _, err := os.Stat(worker1.DirectoryName + "/" + file); err == nil {
		t.Fatalf("File should not exist in client1's directory")
	}
	if _, err := os.Stat(worker2.DirectoryName + "/" + file); err == nil {
		t.Fatalf("File should not exist in client2's directory")
	}

	// Multiple syncs because why not
	for i := 0; i < 3; i++ {
		err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, cfgPath)
		if err != nil {
			t.Fatalf("Client1 sync failed: %v", err)
		}
		err = SyncClient("localhost:8080", "test1", BLOCK_SIZE, cfgPath)
		if err != nil {
			t.Fatalf("Client2 sync failed: %v", err)
		}
	}

	// Verify file metadata and existence on both clients again
	fileMeta1, err = LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client1")
	}
	if len(fileMeta1) != 1 {
		t.Fatalf("Wrong number of entries in client1 meta file")
	}
	if fileMeta1[file].Version != 2 {
		t.Fatalf("Wrong version for file in client1 metadata.")
	}
	if fileMeta1[file].BlockHashList[0] != TOMBSTONE_HASH {
		t.Fatalf("Expected tombstone hash for deleted file in client1 metadata.")
	}

	fileMeta2, err = LoadMetaFromDB(workingDir + "/test1/")
	if err != nil {
		t.Fatalf("Could not load meta file for client2")
	}
	if len(fileMeta2) != 1 {
		t.Fatalf("Wrong number of entries in client2 meta file")
	}
	if fileMeta2[file].Version != 2 {
		t.Fatalf("Wrong version for file in client2 metadata.")
	}
	if fileMeta2[file].BlockHashList[0] != TOMBSTONE_HASH {
		t.Fatalf("Expected tombstone hash for deleted file in client2 metadata.")
	}

	// Verify the file does not exist in both clients' directories again
	if _, err := os.Stat(worker1.DirectoryName + "/" + file); err == nil {
		t.Fatalf("File should not exist in client1's directory")
	}
	if _, err := os.Stat(worker2.DirectoryName + "/" + file); err == nil {
		t.Fatalf("File should not exist in client2's directory")
	}
}
