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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	if rf.votedFor == nil {
		e.Encode(-1)
	} else {
		e.Encode(rf.votedFor)
	}
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
		lastLog := rf.log[len(rf.log)-1]
		DPrintf("candidate(%d) in term %d: LastLogIndex - %d, LastLogTerm - %d, server(%d): LastLogIndex - %d, LastLogTerm - %d\n", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.me, lastLog.Index, lastLog.Term)

		if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && (args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)) {
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

		if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// check prevLogIndex and prevLogTerm
			// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			DPrintf("follower(%d)'s log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm from leader(%d), decrement nextIndex to %d and retry\n", rf.me, args.LeaderId, rf.commitIndex+1)
			reply.Success = false
			reply.NextIndex = rf.commitIndex + 1
			return
		}

		if len(args.Entries) != 0 {
			if args.PrevLogIndex+len(args.Entries) < len(rf.log) && rf.log[args.PrevLogIndex+len(args.Entries)].Index == args.Entries[len(args.Entries)-1].Index && rf.log[args.PrevLogIndex+len(args.Entries)].Term == args.Entries[len(args.Entries)-1].Term {
				// do nothing, repeat AppendEntries RPC
			} else {
				DPrintf("follower(%d) log successfully append entry(last index : %d) from leader(%d) with prevLogIndex %d and prevLogTerm %d in term %d\n", rf.me, args.PrevLogIndex+len(args.Entries), args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Term)
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				rf.persist()
			}
		}

		// heartbeat or successfully apply entry to log
		if args.LeaderCommit > rf.commitIndex {
			index := rf.commitIndex + 1
			if len(rf.log) < args.LeaderCommit {
				rf.commitIndex = len(rf.log) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			DPrintf("server(%d)'s commitIndex update to %d\n", rf.me, rf.commitIndex)

			for ; index <= rf.commitIndex; index++ {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[index].Command,
					CommandIndex: rf.log[index].Index,
					LeaderId:     args.LeaderId,
				}
				DPrintf("server(%d) has applied command of index %d to state machine, entry index %d\n", rf.me, index, rf.log[index].Index)
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
	if rf.killed() {
		return -1, -1, false
	}

	// ========== Lock Region ==========
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != "leader" {
		return -1, -1, false
	}

	currentTerm := rf.currentTerm

	index := len(rf.log)
	entry := Entry{
		Command: command,
		Term:    currentTerm,
		Index:   index,
	}
	DPrintf("Leader(%d) received command with index %d in term %d, replicating...\n", rf.me, index, rf.currentTerm)

	rf.log = append(rf.log, entry)
	rf.persist()
	// ========== Lock Region ==========

	// issue AppendEntries RPCs in parallel to each of the other servers to replicate the entry
	go rf.sendLogEntries(currentTerm)

	return index, currentTerm, true
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
	rf.log = append(rf.log, Entry{
		Command: nil,
		Term:    0,
		Index:   0,
	})

	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Leader发送Heartbeat以及follower定期触发Leader Election均使用go routine
	go rf.periodicCheckElectionTimeout()
	go rf.periodicSendHeartbeat()

	return rf
}

func (rf *Raft) switchToFollower(term int) {
	// candidate or leader switch to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

func (rf *Raft) switchToCandidate() {
	// follower switch to candidate
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = "candidate"
	rf.votedFor = &rf.me
	rf.currentTerm += 1
	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()
	rf.persist()
}

func (rf *Raft) switchToLeader() {
	// candidate switch to leader
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = "leader"

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

		lastLog := rf.log[len(rf.log)-1]
		rf.nextIndex[id] = lastLog.Index + 1
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
func (rf *Raft) initiateLeaderElection() {
	// 捕获成为candidate时的term
	// ========== Lock Region ==========
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	lastLog := rf.log[len(rf.log)-1]
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	rf.mu.Unlock()
	// ========== Lock Region ==========

	var wg sync.WaitGroup
	replyCh := make(chan int)
	voteGranted := 1

	replies := []RequestVoteReply{}

	for id := 0; id < len(rf.peers); id++ {
		replies = append(replies, RequestVoteReply{})
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
		for serverId := range replyCh {
			replyCnt++

			if replies[serverId].Term > currentTerm {
				rf.switchToFollower(replies[serverId].Term)
				return
			}

			if replies[serverId].VoteGranted {
				voteGranted++
			}

			if voteGranted >= (len(rf.peers)+1)/2 {
				// 检查收到reply时candidate的term和state是否有改变
				// ========== Lock Region ==========
				rf.mu.Lock()
				checkTerm := rf.currentTerm
				checkState := rf.state
				rf.mu.Unlock()
				// ========== Lock Region ==========

				if checkTerm == currentTerm && checkState == "candidate" {
					DPrintf("server %d win the election of term %d\n", rf.me, currentTerm)
					rf.switchToLeader()

					go rf.sendLogEntries(currentTerm)
				}
				return
			} else if replyCnt == len(rf.peers)-1 {
				rf.switchToFollower(replies[serverId].Term)
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
		state := rf.state
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		// ========== Lock Region ==========

		if state != "leader" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		go rf.sendLogEntries(currentTerm)

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendLogEntries(currentTerm int) {
	var wg sync.WaitGroup
	peerNum := len(rf.peers)
	replyCh := make(chan int)

	reqs := make([]AppendEntriesArgs, peerNum)
	replies := make([]AppendEntriesReply, peerNum)

	for id := 0; id < peerNum; id++ {
		if id == rf.me {
			continue
		}

		// ========== Lock Region ==========
		rf.mu.Lock()

		if rf.state != "leader" || rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		lastLog := rf.log[len(rf.log)-1]
		prevLogIndex := rf.nextIndex[id] - 1
		prevLogTerm := rf.log[prevLogIndex].Term

		// heartbeat for default
		reqs[id] = AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      []Entry{},
			LeaderCommit: rf.commitIndex,
		}

		if lastLog.Index >= rf.nextIndex[id] {
			reqs[id].Entries = rf.log[rf.nextIndex[id]:]

			DPrintf("leader(%d) sending AppendEntries to server %d in term %d : prevLogIndex - %d; nextIndex - %d; lastLogIndex - %d; leaderCommit - %d;\n", rf.me, id, currentTerm, prevLogIndex, rf.nextIndex[id], lastLog.Index, rf.commitIndex)
		}

		rf.mu.Unlock()
		// ========== Lock Region ==========

		wg.Add(1)
		go func(peerId int) {
			defer wg.Done()

			if ok := rf.sendAppendEntries(peerId, &reqs[peerId], &replies[peerId]); !ok {
				return
			}

			replyCh <- peerId
		}(id)
	}

	go func() {
		replyCnt := 0
		stateReplicated := 1

		for serverId := range replyCh {
			replyCnt++

			if replies[serverId].Success {
				if len(reqs[serverId].Entries) != 0 {
					stateReplicated++
					DPrintf("server(%d) successfully replicate state\n", serverId)

					// ========== Lock Region ==========
					// update nextIndex and matchIndex for success replicating
					rf.mu.Lock()
					if reqs[serverId].PrevLogIndex+len(reqs[serverId].Entries) >= rf.matchIndex[serverId] {
						rf.matchIndex[serverId] = reqs[serverId].PrevLogIndex + len(reqs[serverId].Entries)
						rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
					}
					rf.mu.Unlock()
					// ========== Lock Region ==========
				}
			} else {
				if replies[serverId].Term > currentTerm {
					rf.switchToFollower(replies[serverId].Term)
					return
				} else {
					// ========== Lock Region ==========
					// update nextIndex for conflict server
					rf.mu.Lock()
					rf.nextIndex[serverId] = replies[serverId].NextIndex
					rf.mu.Unlock()
					// ========== Lock Region ==========
				}
			}

			majorityCnt := (len(rf.peers) + 1) / 2
			if stateReplicated >= majorityCnt {
				// ========== Lock Region ==========
				// 检查收到reply时leader的term和state是否有改变, 提交ApplyMsg, 更新commitIndex, 在下一次heartbeat或AppendEntries时发送给follower
				rf.mu.Lock()
				sortedMachIndex := make([]int, len(rf.matchIndex))
				copy(sortedMachIndex, rf.matchIndex)
				sort.Ints(sortedMachIndex)
				majorityCommited := sortedMachIndex[majorityCnt]
				if rf.currentTerm == currentTerm && rf.state == "leader" && majorityCommited > rf.commitIndex && rf.log[majorityCommited].Term == rf.currentTerm {
					rf.commitIndex = majorityCommited
					DPrintf("leader(%d)'s commitIndex update to %d\n", rf.me, rf.commitIndex)

					for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.log[index].Command,
							CommandIndex: rf.log[index].Index,
							LeaderId:     rf.me,
						}
						DPrintf("leader(%d) has applied command of index %d to state machine, entry index %d\n", rf.me, index, rf.log[index].Index)
					}

					rf.lastApplied = rf.commitIndex
				}
				rf.mu.Unlock()
				// ========== Lock Region ==========
			}

			if replyCnt == len(rf.peers)-1 {
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
		startTime := rf.electionStartTime
		timeout := rf.electionTimeout
		state := rf.state
		rf.mu.Unlock()
		// ========== Lock Region ==========

		if state == "leader" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		now := time.Now()
		duration := now.Sub(startTime)
		milliseconds := duration.Milliseconds()
		if milliseconds > int64(timeout) {
			rf.switchToCandidate()

			go rf.initiateLeaderElection()
		}

		time.Sleep(50 * time.Millisecond)
	}
}
