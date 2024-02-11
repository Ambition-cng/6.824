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
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int  // latest term server has seen(initialized to 0 on fiirst boot, increasses monotonically)
	votedFor    *int // candidateId that received vote in current term(or null if none)

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
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == "leader")
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int // candidate reequsting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		// fmt.Printf("candidate %d's term(%d) is greater than server %d(%s - %d), server %d turn to follower and update term\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm, rf.me)
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}

	if args.Term == rf.currentTerm {
		rf.electionStartTime = time.Now()
		rf.electionTimeout = generateRandomTimeout()

		if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
			// 为什么votedFor == CandidateId时也要投票给Candidate？
			// 考虑: 投票的响应包丢包，candidate重复请求投票，应当再次投票
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
			// fmt.Printf("server %d(state : %s), grant vote for server %d in term %d\n", rf.me, rf.state, args.CandidateId, args.Term)
		}
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
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
	Term    int           // leader's term
	Entries []interface{} // log entries to store(empty for heartbeat; may send more than one for effiency)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		// fmt.Printf("leader's term(%d) is greater than server %d(%s), server %d turn to follower and update term\n", args.Term, rf.me, rf.state, rf.me)

		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}

	if args.Term == rf.currentTerm {
		if rf.state == "candidate" {
			// fmt.Printf("got heartbeat from leader, server %d turn to follower and update term\n", rf.me)
			rf.state = "follower"
		}
		rf.electionStartTime = time.Now()
		rf.electionTimeout = generateRandomTimeout()

		reply.Success = true
	} else {
		reply.Success = false
	}

	reply.Term = rf.currentTerm
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// Your code here, if desired.
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = "follower"
	rf.votedFor = nil
	rf.currentTerm = 0
	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()

	// fmt.Printf("server %d: electionTimeout - %d\n", rf.me, rf.electionTimeout)

	// Leader发送Heartbeat以及follower定期触发Leader Election均使用go routine
	go rf.periodicCheckElectionTimeout()
	go rf.periodicSendHeartbeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) switchToFollower(term int) {
	// candidate or leader switch to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = "follower"
	rf.votedFor = nil
	if term > rf.currentTerm {
		rf.currentTerm = term
	}
	rf.electionStartTime = time.Now()
	rf.electionTimeout = generateRandomTimeout()

	// fmt.Printf("server %d switch to follower\n", rf.me)
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

	// fmt.Printf("server %d switch to candidate with term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) switchToLeader() {
	// candidate switch to leader
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = "leader"
	// fmt.Printf("server %d switch to leader in term %d\n", rf.me, rf.currentTerm)
}

// 生成随机的timeout
func generateRandomTimeout() int {
	min := 300
	max := 500
	interval := 20

	newMin := min / interval
	newMax := max / interval
	newRangeVal := new(big.Int).SetInt64(int64(newMax - newMin + 1))

	bigRandNum, _ := rand.Int(rand.Reader, newRangeVal)
	randomNumber := bigRandNum.Int64()*int64(interval) + int64(min)

	return int(randomNumber)
}

// 发布一次Leader Election
func (rf *Raft) initiateLeaderElection() {
	rf.mu.Lock()
	// 捕获成为candidate时的term
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	var wg sync.WaitGroup
	replyCh := make(chan int)
	voteGranted := 1

	args := &RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}
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
				// fmt.Printf("(candidate-%d) during leader election in term %d, no reply was received from the server %d\n", rf.me, currentTerm, peerId)
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
				rf.mu.Lock()
				// 检查收到reply时candidate的term和state是否有改变
				checkTerm := rf.currentTerm
				checkState := rf.state
				rf.mu.Unlock()

				if checkTerm == currentTerm && checkState == "candidate" {
					// fmt.Printf("server %d win the election of term %d\n", rf.me, currentTerm)
					rf.switchToLeader()

					// fmt.Printf("heartbeat from leader %d\n", rf.me)
					go rf.sendHeartbeat(currentTerm)
				}
				return
			} else if replyCnt == len(rf.peers) {
				// fmt.Printf("server %d failed in election of term %d, vote granted: %d\n", rf.me, currentTerm, voteGranted)
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

		rf.mu.Lock()
		state := rf.state
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		if state != "leader" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// fmt.Printf("heartbeat from leader %d in term %d\n", rf.me, currentTerm)

		go rf.sendHeartbeat(currentTerm)

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeat(currentTerm int) {
	var wg sync.WaitGroup
	replyCh := make(chan int)

	args := &AppendEntriesArgs{
		Term:    currentTerm,
		Entries: []interface{}{},
	}
	replies := []AppendEntriesReply{}

	for id := 0; id < len(rf.peers); id++ {
		replies = append(replies, AppendEntriesReply{})
		if id == rf.me {
			continue
		}

		wg.Add(1)
		go func(peerId int) {
			defer wg.Done()

			if ok := rf.sendAppendEntries(peerId, args, &replies[peerId]); !ok {
				// fmt.Printf("(leader-%d) while sending heartbeat in term %d, no reply was received from the server %d\n", rf.me, currentTerm, peerId)
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

			if replyCnt == len(rf.peers) {
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

		rf.mu.Lock()
		startTime := rf.electionStartTime
		timeout := rf.electionTimeout
		state := rf.state
		rf.mu.Unlock()

		if state == "leader" {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		now := time.Now()
		duration := now.Sub(startTime)
		milliseconds := duration.Milliseconds()
		if milliseconds > int64(timeout) {
			// fmt.Printf("server %d's election timeout(%d) exceed, initiate a leader election\n", rf.me, timeout)

			rf.switchToCandidate()

			go rf.initiateLeaderElection()
		}

		time.Sleep(20 * time.Millisecond)
	}
}
