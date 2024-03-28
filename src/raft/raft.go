package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"crypto/rand"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	LeaderId     int
	Snapshot     *Snapshot
}

type Snapshot struct {
	LastIncludedIndex int // index of the last entry in the log that the snapshot replaces (the last entry that the state machine had applied)
	LastIncludedTerm  int // term of lastIncludedIndex
	ServerSnapshot    interface{}
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// all server's state
	currentTerm int     // latest term server has seen(initialized to 0 on fiirst boot, increasses monotonically)
	votedFor    *int    // candidateId that received vote in current term(or null if none)
	log         []Entry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex int // index of highest log entry known to be commited (initialized to 0, increases monotonically)
	lastApplied int // index of highest log applied to state machine (initialized to 0, increases monotonically)

	applyCh chan ApplyMsg // apply channel, for commiting command

	snapshot  *Snapshot
	logOffset int

	// leader's additional state (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	state             string // server's state: follower, candidate, leader
	electionTimeout   int
	electionStartTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = (rf.state == "leader")
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	if rf.votedFor == nil {
		encoder.Encode(-1)
	} else {
		encoder.Encode(rf.votedFor)
	}
	encoder.Encode(rf.log)
	raftState := buffer.Bytes()

	if rf.snapshot == nil {
		rf.persister.SaveRaftState(raftState)
	} else {
		sBuffer := new(bytes.Buffer)
		sEncoder := labgob.NewEncoder(sBuffer)
		sEncoder.Encode(rf.snapshot)
		stateSnapshot := sBuffer.Bytes()
		rf.persister.SaveStateAndSnapshot(raftState, stateSnapshot)
	}
	DPrintf("Server(%d) - size of raft state: %d, len of log: %d\n", rf.me, rf.persister.RaftStateSize(), len(rf.log))
}

// restore previously persisted state.
func (rf *Raft) readPersistState(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor *int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		//   error...
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		if *votedFor == -1 {
			rf.votedFor = nil
			DPrintf("server(%d) reboot after crash, with currentTerm - %d, len(rf.log) - %d, votedFor - -1\n", rf.me, currentTerm, len(log))
		} else {
			rf.votedFor = votedFor
			DPrintf("server(%d) reboot after crash, with currentTerm - %d, len(rf.log) - %d, votedFor - %d\n", rf.me, currentTerm, len(log), *rf.votedFor)
		}
	}
}

func (rf *Raft) readPersistSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snapshot *Snapshot
	if d.Decode(&snapshot) != nil {
		//   error...
	} else {
		rf.snapshot = snapshot
		rf.logOffset = snapshot.LastIncludedIndex + 1
		rf.commitIndex = snapshot.LastIncludedIndex
		rf.lastApplied = snapshot.LastIncludedIndex
	}
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate reequsting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term == rf.currentTerm {
		var lastLogIndex, lastLogTerm int
		if len(rf.log) == 0 {
			lastLogIndex = rf.snapshot.LastIncludedIndex
			lastLogTerm = rf.snapshot.LastIncludedTerm
		} else {
			lastLogIndex = rf.log[len(rf.log)-1].Index
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}

		DPrintf("candidate(%d) in term %d: LastLogIndex - %d, LastLogTerm - %d, server(%d): LastLogIndex - %d, LastLogTerm - %d\n", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, lastLogIndex, lastLogTerm)

		if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			// 为什么votedFor == CandidateId时也要投票给Candidate？
			// 考虑: 投票的响应包丢包，candidate重复请求投票，应当再次投票
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			rf.persist()
			DPrintf("server %d(state : %s), grant vote for server %d in term %d\n", rf.me, rf.state, args.CandidateId, args.Term)

			rf.electionStartTime = time.Now()
			rf.electionTimeout = generateRandomTimeout()
		}
	}

	// args.Term < rf.currentTerm, reply.VoteGranted = false, return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for effiency)
	LeaderCommit int     // leader's commitIndex
}

type AppendEntriesReply struct {
	Term      int  // currentTerm, for leader to update itself
	NextIndex int  // follower's commitIndex+1, for leader to update nextIndex
	Success   bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.persist()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = 0
	if args.Term == rf.currentTerm {
		if rf.state == "candidate" {
			rf.state = "follower"
		}
		rf.electionStartTime = time.Now()
		rf.electionTimeout = generateRandomTimeout()

		followerLogLength := rf.logOffset + len(rf.log)
		followerPrevLogIndex := args.PrevLogIndex - rf.logOffset
		appendEntriesTotalLength := args.PrevLogIndex + len(args.Entries)

		if args.PrevLogIndex >= followerLogLength || followerPrevLogIndex < -1 || (followerPrevLogIndex == -1 && rf.snapshot.LastIncludedTerm != args.PrevLogTerm) || (followerPrevLogIndex >= 0 && rf.log[followerPrevLogIndex].Term != args.PrevLogTerm) {
			// check prevLogIndex and prevLogTerm
			// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			DPrintf("follower(%d)'s log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm from leader(%d), decrement nextIndex to %d and retry\n", rf.me, args.LeaderId, rf.commitIndex+1)
			reply.Success = false
			reply.NextIndex = rf.commitIndex + 1
			return
		}

		if len(args.Entries) != 0 {
			if appendEntriesTotalLength < followerLogLength && rf.log[appendEntriesTotalLength-rf.logOffset].Index == args.Entries[len(args.Entries)-1].Index && rf.log[appendEntriesTotalLength-rf.logOffset].Term == args.Entries[len(args.Entries)-1].Term {
				// do nothing, repeat AppendEntries RPC
			} else {
				DPrintf("follower(%d) log successfully append entry(last index : %d) from leader(%d) with prevLogIndex %d and prevLogTerm %d in term %d\n", rf.me, appendEntriesTotalLength, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term)
				rf.log = rf.log[:followerPrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				rf.persist()
			}
		}

		// heartbeat or successfully apply entry to log
		if args.LeaderCommit > rf.commitIndex {
			if followerLogLength < args.LeaderCommit {
				rf.commitIndex = followerLogLength - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			DPrintf("server(%d)'s commitIndex update to %d\n", rf.me, rf.commitIndex)

			for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[index-rf.logOffset].Command,
					CommandIndex: rf.log[index-rf.logOffset].Index,
					LeaderId:     args.LeaderId,
					Snapshot:     nil,
				}
				DPrintf("server(%d) has applied command of index %d to state machine\n", rf.me, index)
			}
			rf.lastApplied = rf.commitIndex
		}
	} else {
		// Reply false if term(leader) < currentTerm
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients
	Snapshot
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.persist()
	}

	reply.Term = rf.currentTerm
	if args.Term == rf.currentTerm {
		if rf.state == "candidate" {
			rf.state = "follower"
		}
		rf.electionStartTime = time.Now()
		rf.electionTimeout = generateRandomTimeout()

		if rf.snapshot != nil && (args.LastIncludedIndex < rf.snapshot.LastIncludedIndex || (args.LastIncludedIndex == rf.snapshot.LastIncludedIndex && args.LastIncludedTerm == rf.snapshot.LastIncludedTerm)) {
			// repeat InstallSnapshot request or tale snapshot
			return
		}

		if args.LastIncludedIndex-rf.logOffset >= 0 && args.LastIncludedIndex-rf.logOffset < len(rf.log) {
			truncatedLog := rf.log[args.LastIncludedIndex-rf.logOffset+1:]
			newLog := make([]Entry, len(truncatedLog))
			copy(newLog, truncatedLog)

			rf.log = newLog
		} else {
			rf.log = make([]Entry, 0)
		}

		rf.logOffset = args.LastIncludedIndex + 1
		rf.snapshot = &Snapshot{
			LastIncludedIndex: args.LastIncludedIndex,
			LastIncludedTerm:  args.LastIncludedTerm,
			ServerSnapshot:    args.ServerSnapshot,
		}
		rf.persist()

		if args.LastIncludedIndex > rf.commitIndex {
			rf.commitIndex = args.LastIncludedIndex
			rf.lastApplied = args.LastIncludedIndex

			rf.applyCh <- ApplyMsg{
				CommandValid: false,
				Command:      nil,
				CommandIndex: -1,
				LeaderId:     args.LeaderId,
				Snapshot:     rf.snapshot,
			}
			DPrintf("server(%d) has applied a snapshot(Index %d, Term %d) to state machine\n", rf.me, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshot, ok := command.(Snapshot)
	if ok {
		if rf.snapshot == nil || rf.snapshot.LastIncludedIndex < snapshot.LastIncludedIndex {
			snapshot.LastIncludedTerm = rf.log[snapshot.LastIncludedIndex-rf.logOffset].Term
			rf.snapshot = &snapshot

			truncatedLog := rf.log[snapshot.LastIncludedIndex-rf.logOffset+1:]
			rf.logOffset = snapshot.LastIncludedIndex + 1
			newLog := make([]Entry, len(truncatedLog))
			copy(newLog, truncatedLog)

			rf.log = newLog
			rf.persist()
			DPrintf("Server(%d) successfully save a snapshot(Index %d, Term %d), len of log: %d\n", rf.me, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm, len(rf.log))
		}
		return -1, -1, false
	}

	if rf.state != "leader" {
		return -1, -1, false
	}

	index := rf.logOffset + len(rf.log)
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   index,
	}
	DPrintf("Leader(%d) received command with index %d in term %d, replicating...\n", rf.me, index, rf.currentTerm)

	rf.log = append(rf.log, entry)
	rf.persist()

	// issue AppendEntries RPCs in parallel to each of the other servers to replicate the entry
	go rf.sendLogEntries(rf.currentTerm)

	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = "follower"
	rf.votedFor = nil
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.snapshot = nil
	rf.logOffset = 0
	rf.log = append(rf.log, Entry{
		Command: nil,
		Term:    0,
		Index:   0,
	})

	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersistState(persister.ReadRaftState())
	rf.readPersistSnapshot(persister.ReadSnapshot())
	DPrintf("server(%d) reboot after crash, with logOffset - %d\n", rf.me, rf.logOffset)
	if rf.snapshot != nil {
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			Command:      nil,
			CommandIndex: -1,
			LeaderId:     -1,
			Snapshot:     rf.snapshot,
		}
		DPrintf("server(%d) has applied a snapshot(Index %d, Term %d) to to state machine\n", rf.me, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)
	}

	// Leader发送Heartbeat以及follower定期触发Leader Election均使用go routine
	go rf.periodicCheckElectionTimeout()
	go rf.periodicSendHeartbeat()

	return rf
}

func (rf *Raft) switchToFollower(term int) {
	// candidate or leader switch to follower
	rf.state = "follower"
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = nil
		rf.persist()
	}
	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()

	DPrintf("server %d switch to follower\n", rf.me)
}

func (rf *Raft) switchToCandidate(term int) {
	// follower switch to candidate
	rf.state = "candidate"
	rf.votedFor = &rf.me
	rf.currentTerm = term + 1
	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()
	rf.persist()
}

func (rf *Raft) switchToLeader(term int) {
	// candidate switch to leader
	rf.state = "leader"
	rf.currentTerm = term

	peerNum := len(rf.peers)
	if len(rf.nextIndex) != peerNum {
		rf.nextIndex = make([]int, peerNum)
	}
	if len(rf.matchIndex) != peerNum {
		rf.matchIndex = make([]int, peerNum)
	}

	for id := 0; id < peerNum; id++ {
		if id == rf.me {
			continue
		}

		if rf.snapshot != nil && len(rf.log) == 0 {
			rf.nextIndex[id] = rf.snapshot.LastIncludedIndex + 1
		} else {
			rf.nextIndex[id] = rf.log[len(rf.log)-1].Index + 1
		}
		rf.matchIndex[id] = 0
	}
}

// 生成随机的timeout
func generateRandomTimeout() int {
	min := 300
	max := 900

	rangeVal := max - min + 1

	bigRandNum, _ := rand.Int(rand.Reader, big.NewInt(int64(rangeVal)))
	randomNumber := bigRandNum.Int64() + int64(min)

	return int(randomNumber)
}

// 发布一次Leader Election
func (rf *Raft) initiateLeaderElection(currentTerm int) {
	// ========== Lock Region ==========
	rf.mu.Lock()
	var lastLogIndex, lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.snapshot.LastIncludedIndex
		lastLogTerm = rf.snapshot.LastIncludedTerm
	} else {
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()
	// ========== Lock Region ==========

	var wg sync.WaitGroup
	peerNum := len(rf.peers)
	replyCh := make(chan int, peerNum)
	voteGranted := 1

	replies := make([]RequestVoteReply, peerNum)

	for id := 0; id < peerNum; id++ {
		if id == rf.me {
			continue
		}

		wg.Add(1)
		go func(peerId int) {
			defer wg.Done()
			if ok := rf.sendRequestVote(peerId, args, &replies[peerId]); !ok {
				replies[peerId] = RequestVoteReply{
					Term:        currentTerm,
					VoteGranted: false,
				}
				return
			}

			replyCh <- peerId
		}(id)
	}

	go func() {
		replyCnt := 0
		closed := false

		for serverId := range replyCh {
			replyCnt++

			// ========== Lock Region ==========
			rf.mu.Lock()

			if replies[serverId].Term > currentTerm {
				rf.switchToFollower(replies[serverId].Term)
				closed = true
			}

			if replies[serverId].VoteGranted {
				voteGranted++
			}

			if voteGranted >= (peerNum+1)/2 {
				if rf.currentTerm == currentTerm && rf.state == "candidate" {
					// 检查收到reply时candidate的term和state是否有改变
					DPrintf("server %d win the election of term %d\n", rf.me, currentTerm)
					rf.switchToLeader(currentTerm)

					go rf.sendLogEntries(currentTerm)
				}

				closed = true
			}

			if replyCnt == peerNum-1 {
				rf.switchToFollower(replies[serverId].Term)
				closed = true
			}

			rf.mu.Unlock()
			// ========== Lock Region ==========

			if closed {
				return
			}
		}
	}()

	wg.Wait()
	close(replyCh)
}

// Leader发送Heartbeat
func (rf *Raft) periodicSendHeartbeat() {
	for {
		if rf.killed() {
			return
		}

		// ========== Lock Region ==========
		rf.mu.Lock()

		if rf.state != "leader" {
			rf.mu.Unlock()

			time.Sleep(100 * time.Millisecond)
			continue
		}

		go rf.sendLogEntries(rf.currentTerm)

		rf.mu.Unlock()
		// ========== Lock Region ==========

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendLogEntries(currentTerm int) {
	var wg sync.WaitGroup
	peerNum := len(rf.peers)
	replyCh := make(chan int, peerNum)

	reqs := make([]interface{}, peerNum)
	replies := make([]interface{}, peerNum)

	// ========== Lock Region ==========
	rf.mu.Lock()
	if rf.state != "leader" || rf.currentTerm != currentTerm {
		rf.mu.Unlock()
		return
	}

	for id := 0; id < peerNum; id++ {
		if id == rf.me {
			continue
		}

		var lastLogIndex int
		if len(rf.log) == 0 {
			lastLogIndex = rf.snapshot.LastIncludedIndex
		} else {
			lastLogIndex = rf.log[len(rf.log)-1].Index
		}

		if rf.snapshot != nil && rf.nextIndex[id] < rf.logOffset {
			// leader has already discarded the next log entry that it needs to send to a follower
			// send snapshot to follower
			reqs[id] = &InstallSnapshotArgs{
				Term:     currentTerm,
				LeaderId: rf.me,
				Snapshot: Snapshot{
					LastIncludedIndex: rf.snapshot.LastIncludedIndex,
					LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
					ServerSnapshot:    rf.snapshot.ServerSnapshot,
				},
			}

			replies[id] = &InstallSnapshotReply{}

			DPrintf("leader(%d) sending InstallSnapshot to server %d in term %d : LastIncludedIndex - %d; LastIncludedTerm - %d\n", rf.me, id, currentTerm, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm)
		} else {
			prevLogIndex := rf.nextIndex[id] - 1
			var prevLogTerm int
			if rf.snapshot != nil && prevLogIndex == rf.snapshot.LastIncludedIndex {
				prevLogTerm = rf.snapshot.LastIncludedTerm
			} else {
				prevLogTerm = rf.log[prevLogIndex-rf.logOffset].Term
			}

			// heartbeat for default
			args := &AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []Entry{},
				LeaderCommit: rf.commitIndex,
			}

			if lastLogIndex >= rf.nextIndex[id] {
				args.Entries = make([]Entry, len(rf.log[rf.nextIndex[id]-rf.logOffset:]))
				copy(args.Entries, rf.log[rf.nextIndex[id]-rf.logOffset:])

				DPrintf("leader(%d) sending AppendEntries to server %d in term %d : prevLogIndex - %d; prevLogTerm - %d; nextIndex - %d; lastLogIndex - %d; leaderCommit - %d;\n", rf.me, id, currentTerm, prevLogIndex, prevLogTerm, rf.nextIndex[id], lastLogIndex, rf.commitIndex)
			}

			reqs[id] = args
			replies[id] = &AppendEntriesReply{}
		}

		wg.Add(1)
		go func(peerId int) {
			defer wg.Done()

			switch req := reqs[peerId].(type) {
			case *AppendEntriesArgs:
				reply := replies[peerId].(*AppendEntriesReply)
				if ok := rf.sendAppendEntries(peerId, req, reply); !ok {
					return
				}
			case *InstallSnapshotArgs:
				reply := replies[peerId].(*InstallSnapshotReply)
				if ok := rf.sendInstallSnapshot(peerId, req, reply); !ok {
					return
				}
			default:
			}

			replyCh <- peerId
		}(id)
	}

	rf.mu.Unlock()
	// ========== Lock Region ==========

	go func() {
		replyCnt := 0
		stateReplicated := 1
		closed := false

		for serverId := range replyCh {
			replyCnt++

			// ========== Lock Region ==========
			rf.mu.Lock()

			switch reply := replies[serverId].(type) {
			case *AppendEntriesReply:
				req := reqs[serverId].(*AppendEntriesArgs)

				if reply.Success {
					if len(req.Entries) != 0 {
						stateReplicated++
						DPrintf("server(%d) successfully replicate state\n", serverId)

						// update nextIndex and matchIndex for success replicating
						if req.PrevLogIndex+len(req.Entries) >= rf.matchIndex[serverId] {
							rf.matchIndex[serverId] = req.PrevLogIndex + len(req.Entries)
							rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
						}
					}
				} else {
					if reply.Term > currentTerm {
						rf.switchToFollower(reply.Term)
						closed = true
					} else {
						// update nextIndex for conflict server
						rf.nextIndex[serverId] = reply.NextIndex
					}
				}

				majorityCnt := (peerNum + 1) / 2
				if stateReplicated >= majorityCnt {
					// 检查收到reply时leader的term和state是否有改变, 提交ApplyMsg, 更新commitIndex, 在下一次heartbeat或AppendEntries时发送给follower
					sortedMachIndex := make([]int, len(rf.matchIndex))
					copy(sortedMachIndex, rf.matchIndex)
					sort.Ints(sortedMachIndex)
					majorityCommited := sortedMachIndex[majorityCnt]
					if rf.currentTerm == currentTerm && rf.state == "leader" && majorityCommited > rf.commitIndex && rf.log[majorityCommited-rf.logOffset].Term == rf.currentTerm {
						rf.commitIndex = majorityCommited
						DPrintf("leader(%d)'s commitIndex update to %d\n", rf.me, rf.commitIndex)

						for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
							rf.applyCh <- ApplyMsg{
								CommandValid: true,
								Command:      rf.log[index-rf.logOffset].Command,
								CommandIndex: rf.log[index-rf.logOffset].Index,
								LeaderId:     rf.me,
								Snapshot:     nil,
							}
							DPrintf("leader(%d) has applied command of index %d to state machine\n", rf.me, index)
						}

						rf.lastApplied = rf.commitIndex
					}
				}
			case *InstallSnapshotReply:
				if reply.Term > currentTerm {
					rf.switchToFollower(reply.Term)
					closed = true
				} else {
					// follower successfully install snapshot
					DPrintf("server(%d) successfully install snapshot\n", serverId)
					req := reqs[serverId].(*InstallSnapshotArgs)

					rf.nextIndex[serverId] = req.LastIncludedIndex + 1
				}
			default:
			}

			if replyCnt == peerNum-1 {
				closed = true
			}

			rf.mu.Unlock()
			// ========== Lock Region ==========

			if closed {
				return
			}
		}
	}()

	wg.Wait()
	close(replyCh)
}

// 定期检查Election Timeout是否超时
func (rf *Raft) periodicCheckElectionTimeout() {
	for {
		if rf.killed() {
			return
		}

		// ========== Lock Region ==========
		rf.mu.Lock()

		if rf.state == "leader" {
			rf.mu.Unlock()

			time.Sleep(100 * time.Millisecond)
			continue
		}

		now := time.Now()
		duration := now.Sub(rf.electionStartTime)
		milliseconds := duration.Milliseconds()
		if milliseconds > int64(rf.electionTimeout) {
			rf.switchToCandidate(rf.currentTerm)

			go rf.initiateLeaderElection(rf.currentTerm)
		}

		rf.mu.Unlock()
		// ========== Lock Region ==========

		time.Sleep(50 * time.Millisecond)
	}
}
