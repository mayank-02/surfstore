package surfstore

import (
	context "context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	serverStatusMutex *sync.RWMutex // Mutex for serverStatus
	serverStatus      ServerStatus  // Crashed, follower, or leader

	raftStateMutex *sync.RWMutex      // Mutex for Raft state
	id             int64              // Server ID
	metaStore      *MetaStore         // MetaStore object
	term           int64              // Latest term server has seen
	log            []*UpdateOperation // Log of operations
	commitIndex    int64              // Index of highest log entry known to be committed
	lastApplied    int64              // Index of highest log entry applied to state machine
	rpcConns       []*grpc.ClientConn // RPC connections to servers, including this one
	grpcServer     *grpc.Server       // gRPC server listening for incoming connections
	clusterMembers []string           // Servers in the cluster, including this one

	/*-------------- Leader only --------------*/
	nextIndex  map[int64]int64 // For each server, index of the next log entry to send to that server
	matchIndex map[int64]int64 // For each server, index of highest log entry known to be replicated on server

	/*-------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool // Servers that this server is unreachable from
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// If a node is crashed, it should return an error
	if s.isCrashed() {
		log.Printf("[Server %v] In crashed state, cannot respond to GetFileInfoMap()", s.id)
		return nil, ErrServerCrashed
	}

	// If a node is not a leader, it should return an error
	if !s.isLeader() {
		log.Printf("[Server %v] Not a leader, cannot respond to GetFileInfoMap()", s.id)
		return nil, ErrNotLeader
	}

	// Ensure that the majority of servers are up
	for {
		majority, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Printf("[Server %v] Failed to ensure majority: %v", s.id, err)
			return nil, err
		}
		log.Printf("[Server %v] Is quorum available? %v", s.id, majority.Flag)
		if majority.Flag {
			break
		}
	}

	log.Printf("[Server %v] Getting file info map", s.id)

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// If a node is crashed, it should return an error
	if s.isCrashed() {
		log.Printf("[Server %v] In crashed state, cannot respond to GetBlockStoreMap()", s.id)
		return nil, ErrServerCrashed
	}

	// If a node is not a leader, it should return an error
	if !s.isLeader() {
		log.Printf("[Server %v] Not a leader, cannot respond to GetBlockStoreMap()", s.id)
		return nil, ErrNotLeader
	}

	// Ensure that the majority of servers are up
	for {
		majority, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Printf("[Server %v] Failed to ensure majority: %v", s.id, err)
			return nil, err
		}
		log.Printf("[Server %v] Is quorum available? %v", s.id, majority.Flag)
		if majority.Flag {
			break
		}
	}

	log.Printf("[Server %v] Getting block store map for hashes: %v", s.id, hashes.Hashes)

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// If a node is crashed, it should return an error
	if s.isCrashed() {
		log.Printf("[Server %v] In crashed state, cannot respond to GetBlockStoreAddrs()", s.id)
		return nil, ErrServerCrashed
	}

	// If a node is not a leader, it should return an error
	if !s.isLeader() {
		log.Printf("[Server %v] Not a leader, cannot respond to GetBlockStoreAddrs()", s.id)
		return nil, ErrNotLeader
	}

	// Ensure that the majority of servers are up
	for {
		majority, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Printf("[Server %v] Failed to ensure majority: %v", s.id, err)
			return nil, err
		}
		log.Printf("[Server %v] Is quorum available? %v", s.id, majority.Flag)
		if majority.Flag {
			break
		}
	}

	log.Printf("[Server %v] Getting block store addresses", s.id)

	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

// The SetLeader() function should emulate an election, so after calling it on a node it should
// set all the state as if that node had just won an election.
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// If a node is crashed, it should do nothing
	if s.isCrashed() {
		log.Printf("[Server %v] Crashed, cannot set as leader", s.id)
		return nil, ErrServerCrashed
	}

	// Do nothing if node is already the leader
	if s.isLeader() {
		log.Printf("[Server %v] Already a leader", s.id)
		return &Success{Flag: true}, nil
	}

	s.serverStatusMutex.Lock()
	s.raftStateMutex.Lock()
	// Set the node to be the leader
	s.serverStatus = ServerStatus_LEADER
	// Increment term
	s.term += 1
	// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
	s.nextIndex = make(map[int64]int64)
	s.matchIndex = make(map[int64]int64)
	for i := range s.clusterMembers {
		s.nextIndex[int64(i)] = int64(len(s.log))
		s.matchIndex[int64(i)] = -1
	}
	log.Printf("[Server %v] Set to leader, incremented term to %v, reset nextIndex to %v and matchIndex to %v", s.id, s.term, s.nextIndex, s.matchIndex)
	s.raftStateMutex.Unlock()
	s.serverStatusMutex.Unlock()

	// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
	// each new leader must commit a no-op entry in its log at the beginning of its term. encouraged to
	// implement this by making a simple call to UpdateFile(), passing nil as the argument for file metadata
	log.Printf("[Server %v] Sending initial empty AppendEntries RPCs...", s.id)
	_, err := s.UpdateFile(ctx, nil)

	// Server was successfully established as leader
	log.Printf("[Server %v] Successfully established as leader", s.id)
	return &Success{Flag: true}, err
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// If a node is crashed, it should return an error
	if s.isCrashed() {
		log.Printf("[Server %v] In crashed state, cannot respond to UpdateFile()", s.id)
		return nil, ErrServerCrashed
	}

	// If a node is not a leader, it should return an error
	if !s.isLeader() {
		log.Printf("[Server %v] Not a leader, cannot respond to GetFileInfoMap()", s.id)
		return nil, ErrNotLeader
	}

	// If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine

	// Append the entry to the log
	s.raftStateMutex.Lock()
	s.log = append(s.log, &UpdateOperation{Term: s.term, FileMetaData: filemeta})
	log.Printf("[Server %v] Appended entry {%v} to log. Log = %v", s.id, filemeta, s.log)
	s.raftStateMutex.Unlock()

	// Ensure that the majority of servers are up
	log.Printf("[Server %v] Trying to replicate request on majority of the servers...", s.id)
	for {
		result, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			log.Printf("[Server %v] Failed to replicate request: %v", s.id, err)
			return nil, err
		}
		log.Printf("[Server %v] Is quorum available? %v", s.id, result.Flag)
		if result.Flag {
			break
		}
	}

	log.Printf("[Server %v] Request replicated on majority of the servers", s.id)

	s.applyLogsToStateMachine(ctx)

	// A log entry is committed once the leader that created the entry has
	// replicated it on a majority of the servers
	s.advanceCommitIndex()

	log.Printf("[Server %v] Committed entry %v, commit index: %v", s.id, filemeta, s.commitIndex)

	if filemeta == nil {
		return nil, nil
	}

	// Apply the latest entry to the state machine
	return s.metaStore.UpdateFile(ctx, filemeta)
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	log.Printf("[Server %v] Received AppendEntries RPC {%v} from leader %v", s.id, input, input.LeaderId)

	response := &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: 0}

	// Do nothing if a node is crashed or marked unreachable from the leader
	s.raftStateMutex.RLock()
	if s.isCrashed() || s.unreachableFrom[input.LeaderId] {
		log.Printf("[Server %v] Crashed or unreachable, cannot append entries", s.id)
		s.raftStateMutex.RUnlock()
		return nil, ErrServerCrashedUnreachable
	}

	// Reply false if leader's term < current term (§5.1)
	if input.Term < s.term {
		log.Printf("[Server %v] Leader's term (%v) < current term (%v), cannot append entries", s.id, input.Term, s.term)
		s.raftStateMutex.RUnlock()
		return response, nil
	}

	// Reply false if log doesn't contain an entry at prevLogIndex or whose term doesn't match prevLogTerm (§5.3)
	validHistory := (input.PrevLogIndex == -1 ||
		(input.PrevLogIndex < int64(len(s.log)) && s.log[input.PrevLogIndex].Term == input.PrevLogTerm))

	if !validHistory {
		log.Printf("[Server %v] Invalid previous log, cannot append entries", s.id)
		s.raftStateMutex.RUnlock()
		return response, nil
	}
	s.raftStateMutex.RUnlock()

	s.raftStateMutex.Lock()

	// All Servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if input.Term > s.term {
		s.term = input.Term
		s.serverStatus = ServerStatus_FOLLOWER
	}

	// Append the entries to the log
	start := int(input.PrevLogIndex) + 1
	for i := start; i < start+len(input.Entries); i++ {
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		entry := input.Entries[i-start]
		if i < len(s.log) && s.log[i].Term != entry.Term {
			log.Printf("[Server %v] Conflicting entry at index %v, deleting remaining entries", s.id, i)
			s.log = s.log[:i]
		}

		// Append any new entries not already in the log
		if i >= len(s.log) {
			s.log = append(s.log, entry)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
		log.Printf("[Server %v] Updated commit index to %v", s.id, s.commitIndex)
	}

	// Update the response
	response.Success = true
	response.MatchedIndex = int64(len(s.log) - 1)

	s.raftStateMutex.Unlock()

	log.Printf("[Server %v] Appended entries successfully. Log = %v. Commit Index = %v", s.id, s.log, s.commitIndex)

	s.applyLogsToStateMachine(ctx)

	return response, nil
}

// Sends a round of AppendEntries to all other nodes. The leader will attempt to replicate logs to all other nodes when this is called.
// It can be called even when there are no entries to replicate.
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// Do nothing if a node is crashed
	if s.isCrashed() {
		log.Printf("[Server %v] Crashed, cannot send SendHeartbeat()", s.id)
		return nil, ErrServerCrashed
	}

	// Do nothing if node is not a leader
	if !s.isLeader() {
		log.Printf("[Server %v] Not a leader, cannot send SendHeartbeat()", s.id)
		return nil, ErrNotLeader
	}

	var wg sync.WaitGroup
	syncedFollowers := int32(0)
	successChan := make(chan bool, len(s.rpcConns)-1)

	for i := range s.rpcConns {
		if int64(i) == s.id {
			continue
		}
		wg.Add(1)
		go func(serverIndex int) {
			defer wg.Done()
			err := s.syncFollower(int64(serverIndex))
			if err != nil {
				log.Printf("[Server %v] SendHeartbeat failed for server %v: %v", s.id, serverIndex, err)
				successChan <- false
				return
			}
			atomic.AddInt32(&syncedFollowers, 1)
			successChan <- true
			log.Printf("[Server %v] SendHeartbeat successful for server %v", s.id, serverIndex)
		}(i)
	}

	// Block until all available followers have been synced
	wg.Wait()
	close(successChan)

	// Check if majority of servers are available
	if syncedFollowers < int32(len(s.clusterMembers)/2) {
		log.Printf("[Server %v] Not enough servers available for a quorum", s.id)
		return &Success{Flag: false}, nil
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) advanceCommitIndex() {
	s.raftStateMutex.Lock()
	// Leaders: If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
	for N := len(s.log) - 1; N > int(s.commitIndex); N-- {
		if s.majorityQuorum(int64(N)) {
			s.commitIndex = int64(N)
			break
		}
	}
	log.Printf("[Server %v] Updated commit index to %v because of majority quorum", s.id, s.commitIndex)
	s.raftStateMutex.Unlock()
}

// Return true if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm
// Note: Caller holds lock
func (s *RaftSurfstore) majorityQuorum(N int64) bool {
	count := 0
	for _, matchIdx := range s.matchIndex {
		if matchIdx >= N {
			count++
		}
	}
	return count >= len(s.clusterMembers)/2 && s.log[N].Term == s.term
}

func (s *RaftSurfstore) syncFollower(followerId int64) error {
	for {
		// Do nothing if a node is crashed
		if s.isCrashed() {
			log.Printf("[Server %v] Crashed, cannot sync follower %v", s.id, followerId)
			return ErrServerCrashed
		}

		// Do nothing if node is not a leader
		if !s.isLeader() {
			log.Printf("[Server %v] Not a leader, cannot sync follower %v", s.id, followerId)
			return ErrNotLeader
		}

		// Send AppendEntries RPC to server i
		s.raftStateMutex.RLock()
		prevLogIndex := s.nextIndex[int64(followerId)] - 1
		prevLogTerm := int64(0)
		if prevLogIndex > -1 {
			prevLogTerm = s.log[prevLogIndex].Term
		}
		appendEntryInput := &AppendEntryInput{
			Term:         s.term,
			LeaderId:     s.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
			Entries:      s.log[s.nextIndex[followerId]:],
			LeaderCommit: s.commitIndex,
		}
		s.raftStateMutex.RUnlock()
		response, err := NewRaftSurfstoreClient(s.rpcConns[followerId]).AppendEntries(context.Background(), appendEntryInput)
		if err != nil {
			if err == ErrServerCrashedUnreachable {
				log.Printf("[Server %v] Follower %v is unreachable, retrying...", s.id, followerId)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			log.Printf("[Server %v] Error sending AppendEntries to follower %v: %v", s.id, followerId, err)
			return err
		}

		// If successful: update nextIndex and matchIndex for follower
		if response.Success {
			s.raftStateMutex.Lock()
			s.nextIndex[followerId] = response.MatchedIndex + 1
			s.matchIndex[followerId] = response.MatchedIndex
			s.raftStateMutex.Unlock()
			return nil
		} else {
			// If AppendEntries fails because of term mismatch: set term to leader's term and convert to follower
			if response.Term > s.term {
				s.raftStateMutex.Lock()
				s.term = response.Term
				s.serverStatus = ServerStatus_FOLLOWER
				s.raftStateMutex.Unlock()
				log.Printf("[Server %v] Follower %v's term greater, stepping down as leader", s.id, followerId)
				return ErrNotLeader
			}

			// If a follower’s log is inconsistent with the leader’s, the AppendEntries consistency check will fail in the next AppendEntries RPC. After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC. Eventually nextIndex will reach a point where the leader and follower logs match. When this happens,AppendEntrieswill succeed, which removes any conflicting entries in the follower’s log and appends entries fromthe leader’s log (if any). Once AppendEntries succeeds, the follower’s log is consistentwith the leader’s, and it will remain that way for the rest of the term.
			s.raftStateMutex.Lock()
			s.nextIndex[followerId] = max(0, s.nextIndex[followerId]-1)
			s.raftStateMutex.Unlock()
			log.Printf("[Server %v] AppendEntries failed for follower %v, retrying...", s.id, followerId)
		}
	}
}

func (s *RaftSurfstore) applyLogsToStateMachine(ctx context.Context) {
	s.raftStateMutex.Lock()
	defer s.raftStateMutex.Unlock()

	// All Servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	log.Printf("[Server %v] Applying logs from %v to %v", s.id, s.lastApplied, s.commitIndex)
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		operation := s.log[s.lastApplied]

		// Skip applying a no-op entry
		if operation.FileMetaData == nil {
			log.Printf("[Server %v] Applied entry: no-op", s.id)
			continue
		}

		// Apply the entry to the state machine
		v, err := s.metaStore.UpdateFile(ctx, operation.FileMetaData)
		if err != nil {
			log.Fatalf("[Server %v] Error applying entry %v: %v", s.id, operation.FileMetaData, err)
		}
		if v.Version != operation.FileMetaData.Version {
			log.Printf("[Server %v] Error applying entry %v: version mismatch", s.id, operation.FileMetaData)

		} else {
			log.Printf("[Server %v] Applied entry: %v", s.id, operation.FileMetaData.Filename)
		}
	}
}

func (s *RaftSurfstore) isCrashed() bool {
	s.serverStatusMutex.RLock()
	defer s.serverStatusMutex.RUnlock()

	return s.serverStatus == ServerStatus_CRASHED
}

func (s *RaftSurfstore) isLeader() bool {
	s.serverStatusMutex.RLock()
	defer s.serverStatusMutex.RUnlock()

	return s.serverStatus == ServerStatus_LEADER
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	if len(servers.ServerIds) == 0 {
		s.unreachableFrom = make(map[int64]bool)
		log.Printf("[Server %v] Reachable from all servers", s.id)
	} else {
		for _, serverId := range servers.ServerIds {
			s.unreachableFrom[serverId] = true
		}
		log.Printf("[Server %v] Unreachable from %v", s.id, servers)
	}
	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("[Server %v] Crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("[Server %v] Restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
