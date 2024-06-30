package surftest

import (
	"os"
	"testing"
	"time"

	"github.com/mayank-02/surfstore/pkg/surfstore"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const (
	ThreeNodesConfigPath = "./config_files/3nodes.json"
	FourNodesConfigPath  = "./config_files/4nodes.json"
	FiveNodesConfigPath  = "./config_files/5nodes.json"
)

func TestRaftSetLeader(t *testing.T) {
	// Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// Heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// All should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		// Verify server states
		if idx == leaderIdx {
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// Heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// All should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server %v should be in term %d", idx, 2)
		}
		// Verify server states
		if idx == leaderIdx {
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	// Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %v", idx, err)
		}
	}
}

func TestRaftNetworkPartition(t *testing.T) {
	t.Log("Leader 1 gets 1 request while the majority of the cluster is unreachable.")
	t.Log("As a result of a (one way) network partition, leader1 ends up with the minority.")
	t.Log("Leader 2 from the majority is elected")

	test := InitTest(FiveNodesConfigPath)
	defer EndTest(test)

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	A := 0 // A is the leader
	C := 2
	E := 4

	leaderIdx := A
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Partition happens: C, D, E are unreachable from A and B
	for i := C; i <= E; i++ {
		test.Clients[i].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{
			ServerIds: []int64{0, 1},
		})
	}

	blockChan := make(chan bool)

	// A gets an entry and pushes to A and B
	go func() {
		// This should block though and fail to commit when getting the RPC response from the new leader
		_, _ = test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
		blockChan <- false
	}()

	go func() {
		<-time.NewTimer(5 * time.Second).C
		blockChan <- true
	}()

	if !(<-blockChan) {
		t.Fatalf("Request did not block")
	}

	// C becomes leader
	leaderIdx = C
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// All servers can communicate with each other
	for i := C; i <= E; i++ {
		test.Clients[i].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: []int64{}})
	}

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(time.Second)

	// Every node should have an empty log
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})

	// Leaders should not commit entries that were created in previous terms
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	term := int64(2)

	for idx, server := range test.Clients {
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestGradescopeCase(t *testing.T) {
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	// Leader 1 gets a request while a minority of the cluster is down
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)

	// Leader 1 crashes before sending a heartbeat
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	// Other crashed nodes are restored. Leader 2 gets a request.
	leaderIdx = 1
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Leader 1 is restored
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1
	goldenMeta[filemeta2.Filename] = filemeta2

	goldenLog := []*surfstore.UpdateOperation{
		{
			Term:         1,
			FileMetaData: nil,
		},
		{
			Term:         1,
			FileMetaData: filemeta1,
		},
		{
			Term:         2,
			FileMetaData: nil,
		},
		{
			Term:         2,
			FileMetaData: filemeta2,
		},
	}

	var leader bool
	term := int64(2)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestMinorityServerDownForMultipleUpdates(t *testing.T) {
	// Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	// Set initial leader
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Update file on the leader
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)

	// Crash one server
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Restore crashed server
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1
	goldenMeta[filemeta2.Filename] = filemeta2

	goldenLog := []*surfstore.UpdateOperation{
		{
			Term:         1,
			FileMetaData: nil,
		},
		{
			Term:         1,
			FileMetaData: filemeta1,
		},
		{
			Term:         1,
			FileMetaData: filemeta2,
		},
	}

	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestMinorityServerDownAndLeaderSwitches(t *testing.T) {
	// Setup
	test := InitTest(FiveNodesConfigPath)
	defer EndTest(test)

	// Set initial leader
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[4].Crash(test.Context, &emptypb.Empty{})

	// Update file on the leader
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[3].Crash(test.Context, &emptypb.Empty{})
	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Restore crashed server
	test.Clients[4].Restore(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMetaFull := make(map[string]*surfstore.FileMetaData)
	goldenMetaFull[filemeta1.Filename] = filemeta1
	goldenMetaFull[filemeta2.Filename] = filemeta2

	goldenMeta3 := make(map[string]*surfstore.FileMetaData)
	goldenMeta3[filemeta1.Filename] = filemeta1

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})

	goldenLog3 := make([]*surfstore.UpdateOperation, 0)
	goldenLog3 = append(goldenLog3, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog3 = append(goldenLog3, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	var leader bool
	var term int64
	var gl []*surfstore.UpdateOperation
	var gm map[string]*surfstore.FileMetaData

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}

		if idx == 3 {
			gl = goldenLog3
			gm = goldenMeta3
			term = int64(1)
		} else {
			term = int64(2)
			gm = goldenMetaFull
			gl = goldenLog
		}

		_, err := CheckInternalState(&leader, &term, gl, gm, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestUpdateFileOnNonLeader(t *testing.T) {
	//Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	_, err := test.Clients[1].UpdateFile(test.Context, filemeta1)

	if err == nil {
		t.Fatalf("Expected UpdateFile return an error but got nil")
	}
	state, _ := test.Clients[1].GetInternalState(test.Context, &emptypb.Empty{})
	if len(state.Log) > 1 {
		t.Fatalf("Expected log's len to be 1 but got %d", len(state.Log))
	}
}

func TestGetFileInfoMap(t *testing.T) {
	//Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	var hashlist []string
	hashlist = append(hashlist, "abc")
	hashlist = append(hashlist, "edf")
	hashlist = append(hashlist, "123")

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: hashlist,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	for idx := range test.Clients {
		serverFileInfoMap, err := test.Clients[idx].GetFileInfoMap(test.Context, &emptypb.Empty{})
		if idx == leaderIdx && err != nil {
			t.Fatalf("Error getting file info map on server %d: %s", idx, err.Error())
		} else if idx != leaderIdx && err == nil {
			t.Fatal("Non-leader server should not get success on GetFileInfoMap")
		}
		if serverFileInfoMap != nil {
			surfstore.PrintMetaMap(serverFileInfoMap.FileInfoMap)
		}
	}
}

func TestRaftRetry(t *testing.T) {
	//Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	// Node 0 is set as leader (term 1)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 1 is crashes
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}

	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testFile3",
		Version:       1,
		BlockHashList: nil,
	}

	// Node 0 call update file multiple times
	test.Clients[0].UpdateFile(test.Context, filemeta1)
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].UpdateFile(test.Context, filemeta2)
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[0].UpdateFile(test.Context, filemeta3)
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 2 restore
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})

	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)

	goldenMeta[filemeta1.Filename] = filemeta1
	goldenMeta[filemeta2.Filename] = filemeta2
	goldenMeta[filemeta3.Filename] = filemeta3
	goldenLog := make([]*surfstore.UpdateOperation, 0)

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta2,
	})

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta3,
	})

	leaderIdx := 0
	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestRaftCurrentTermIsGreaterThanTheInputTerm(t *testing.T) {
	//Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	// Node 0 is set as leader (term 1)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 0 is unreachable from node 1
	test.Clients[0].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: []int64{1}})

	// Node 1 is set as leader (term 2)
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 0 tries to appendEntries on Node 1
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	//Updatefile should fail
	test.Clients[0].UpdateFile(test.Context, filemeta1)

	t.Log("SOMETHING")

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})

	leader := false
	term := int64(2)
	_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, test.Clients[2], test.Context)
	if err != nil {
		t.Fatalf("Error checking state for server %d: %v", 0, err.Error())
	}

	leader = true
	_, err = CheckInternalState(&leader, &term, goldenLog, goldenMeta, test.Clients[1], test.Context)
	if err != nil {
		t.Fatalf("Error checking state for server %d: %s", 0, err.Error())
	}

	goldenLog[1] = &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	}
	leader = false
	term = int64(2)
	_, err = CheckInternalState(&leader, &term, goldenLog, goldenMeta, test.Clients[0], test.Context)
	if err != nil {
		t.Fatalf("Error checking state for server %d: %s", 0, err.Error())
	}
}

func TestRaftInconsistentLog(t *testing.T) {
	//Setup
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	// Node 0 is set as leader (term 1)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 0 is unreachable from node 1
	test.Clients[0].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: []int64{1}})

	// Node 1 is set as leader (term 2)
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 0 tries to appendEntries on Node 1
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	//updatefile should fail
	test.Clients[0].UpdateFile(test.Context, filemeta1)

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         3,
		FileMetaData: nil,
	})

	// Node 2 is set as leader term 3
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 2 call update file2
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}

	goldenMeta[filemeta2.Filename] = filemeta2
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         3,
		FileMetaData: filemeta2,
	})
	test.Clients[2].UpdateFile(test.Context, filemeta2)

	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})

	leaderIdx := 2
	var leader bool
	term := int64(3)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestRaftWithEvenNodes(t *testing.T) {
	test := InitTest("./config_files/4nodes.json")
	defer EndTest(test)

	// Node 0 is set as leader (term 1)
	test.Clients[0].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[0].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 0 is unreachable from node 1
	test.Clients[0].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: []int64{1}})

	// Node 1 is set as leader (term 2)
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[1].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 0 tries to appendEntries on Node 1
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	//updatefile should fail
	test.Clients[0].UpdateFile(test.Context, filemeta1)

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: nil,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: nil,
	})

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         3,
		FileMetaData: nil,
	})

	// Node 2 is set as leader term 3
	test.Clients[2].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Node 2 call update file2
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}

	goldenMeta[filemeta2.Filename] = filemeta2
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         3,
		FileMetaData: filemeta2,
	})
	test.Clients[2].UpdateFile(test.Context, filemeta2)

	test.Clients[2].SendHeartbeat(test.Context, &emptypb.Empty{})
	leaderIdx := 2
	var leader bool
	term := int64(3)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestLeaderFailureRecovery(t *testing.T) {
	t.Log("Leader fails and recovers. Ensure state consistency.")
	test := InitTest(ThreeNodesConfigPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	worker := InitDirectoryWorker("test0", SRC_PATH)
	defer worker.CleanUp()

	file := "hello.txt"
	err := worker.AddFile(file)
	if err != nil {
		t.FailNow()
	}

	// Leader syncs
	t.Log("[Orchestrator] Leader syncs")
	err = SyncClient("localhost:8080", "test0", BLOCK_SIZE, ThreeNodesConfigPath)
	if err != nil {
		t.Fatalf("Sync failed")
	}

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})

	// Elect new leader
	newLeaderIdx := 1
	test.Clients[newLeaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[newLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Recover old leader
	test.Clients[leaderIdx].Restore(test.Context, &emptypb.Empty{})
	test.Clients[newLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Verify state consistency
	workingDir, _ := os.Getwd()
	_, err = os.Stat(workingDir + "/test0/" + META_FILENAME)
	if err != nil {
		t.Fatalf("Could not find meta file for client")
	}
	fileMeta, err := LoadMetaFromDB(workingDir + "/test0/")
	if err != nil {
		t.Fatalf("Could not load meta file for client")
	}
	if len(fileMeta) != 1 {
		t.Fatalf("Wrong number of entries in client meta file")
	}
	if fileMeta == nil || fileMeta[file].Version != 1 {
		t.Fatalf("Wrong version for file in client metadata.")
	}
}

func TestFollowerCatchUpAfterPartition(t *testing.T) {
	t.Log("Follower catches up after network partition is resolved.")
	test := InitTest(FiveNodesConfigPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	// Partition network
	for i := 3; i <= 4; i++ {
		test.Clients[i].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{
			ServerIds: []int64{0, 1, 2},
		})
	}

	// Update file on leader
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Recover partition
	for i := 3; i <= 4; i++ {
		test.Clients[i].MakeServerUnreachableFrom(test.Context, &surfstore.UnreachableFromServers{ServerIds: []int64{}})
	}

	// New leader election
	newLeaderIdx := 2
	test.Clients[newLeaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[newLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Ensure all nodes have consistent log
	goldenLog := []*surfstore.UpdateOperation{
		{
			Term:         1,
			FileMetaData: nil,
		},
		{
			Term:         1,
			FileMetaData: filemeta1,
		},
		{
			Term:         2,
			FileMetaData: nil,
		},
	}
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1
	term := int64(2)
	for idx, server := range test.Clients {
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestMultipleLeaderFailures(t *testing.T) {
	t.Log("Test multiple leader failures and recovery.")
	test := InitTest(FiveNodesConfigPath)
	defer EndTest(test)

	// Initial leader election
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	// Update file on leader
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Leader crashes
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	newLeaderIdx := 1
	test.Clients[newLeaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[newLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}

	// Update file on new leader
	test.Clients[newLeaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[newLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// New leader crashes
	test.Clients[newLeaderIdx].Crash(test.Context, &emptypb.Empty{})
	anotherLeaderIdx := 2
	test.Clients[anotherLeaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[anotherLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testFile3",
		Version:       1,
		BlockHashList: nil,
	}

	// Update file on another leader
	test.Clients[anotherLeaderIdx].UpdateFile(test.Context, filemeta3)
	test.Clients[anotherLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Restore all crashed leaders
	test.Clients[leaderIdx].Restore(test.Context, &emptypb.Empty{})
	test.Clients[newLeaderIdx].Restore(test.Context, &emptypb.Empty{})
	test.Clients[anotherLeaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Ensure all nodes have consistent state
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1
	goldenMeta[filemeta2.Filename] = filemeta2
	goldenMeta[filemeta3.Filename] = filemeta3

	goldenLog := []*surfstore.UpdateOperation{
		{
			Term:         1,
			FileMetaData: nil,
		},
		{
			Term:         1,
			FileMetaData: filemeta1,
		},
		{
			Term:         2,
			FileMetaData: nil,
		},
		{
			Term:         2,
			FileMetaData: filemeta2,
		},
		{
			Term:         3,
			FileMetaData: nil,
		},
		{
			Term:         3,
			FileMetaData: filemeta3,
		},
	}

	term := int64(3)
	for idx, server := range test.Clients {
		_, err := CheckInternalState(nil, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %v: %v", idx, err)
		}
	}
}

func TestEvenNumberOfServersWithCrashes(t *testing.T) {
	// Setup a cluster with an even number of servers (4 nodes)
	test := InitTest(FourNodesConfigPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// Heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	// Crash one of the servers
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	// Attempt to update file metadata on the leader
	_, err := test.Clients[leaderIdx].UpdateFile(test.Context, &surfstore.FileMetaData{
		Filename:      "file2",
		Version:       1,
		BlockHashList: []string{"hash2"},
	})
	if err != nil {
		t.Fatalf("Leader failed to handle client request after crash: %v", err)
	}

	// Restore the crashed server
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})

	// Heartbeat to ensure logs are synchronized
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	fileInfo, err := test.Clients[leaderIdx].GetFileInfoMap(test.Context, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Restored server failed to get file info: %v", err)
	}
	if fileInfo.FileInfoMap["file2"].Version != 1 {
		t.Fatalf("Restored server has incorrect file version: %v", fileInfo.FileInfoMap["file2"].Version)
	}
}
