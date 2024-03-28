package shardmaster

import (
	"container/list"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	commandIndex int                       // index of highest command known to be executed
	replyChanMap map[int]chan CommandReply // used to receive replies for Join, Leave or Query commands, key - commandIndex, value - channel of reply
	replyRecord  map[int]int               // record of previous request, to ensure that ShardMaster executes each request just once.
	configs      []Config                  // indexed by config num

	distribution map[int]*list.List // distribution of shards
}

type Op struct {
	Operation         interface{}
	RequestIdentifier Identifier
}

type CommandReply struct {
	Config Config
}

type DistributionEntry struct {
	Gid     int
	Replica *list.List
}

func (sm *ShardMaster) checkRepeatRequest(identifier Identifier) bool {
	// ========== Lock Region ==========
	sm.mu.Lock()
	requestId, ok := sm.replyRecord[identifier.ClientId]
	sm.mu.Unlock()
	// ========== Lock Region ==========

	if ok {
		if identifier.RequestId == requestId {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) executeClientRequest(command Op) bool {
	ret := false

	index, term, is_leader := sm.rf.Start(command)
	if is_leader {
		replyChan := make(chan CommandReply, 1)
		defer close(replyChan)

		// ========== Lock Region ==========
		sm.mu.Lock()
		sm.replyChanMap[index] = replyChan
		sm.mu.Unlock()
		// ========== Lock Region ==========

		// waiting raft executing command
		select {
		case <-replyChan:
			checkTerm, checkLeader := sm.rf.GetState()

			if checkTerm == term && checkLeader {
				ret = true
			}
		case <-time.After(300 * time.Millisecond):
		}

		// ========== Lock Region ==========
		sm.mu.Lock()
		delete(sm.replyChanMap, index)
		sm.mu.Unlock()
		// ========== Lock Region ==========
	}

	return ret
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if sm.checkRepeatRequest(args.Identifier) {
		reply.Err = "OK"
	} else {
		reply.WrongLeader = true
		command := Op{
			Operation:         args,
			RequestIdentifier: args.Identifier,
		}

		if sm.executeClientRequest(command) {
			reply.WrongLeader = false
			reply.Err = "OK"
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sm.checkRepeatRequest(args.Identifier) {
		reply.Err = "OK"
	} else {
		reply.WrongLeader = true
		command := Op{
			Operation:         args,
			RequestIdentifier: args.Identifier,
		}

		if sm.executeClientRequest(command) {
			reply.WrongLeader = false
			reply.Err = "OK"
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if sm.checkRepeatRequest(args.Identifier) {
		reply.Err = "OK"
	} else {
		reply.WrongLeader = true
		command := Op{
			Operation:         args,
			RequestIdentifier: args.Identifier,
		}

		if sm.executeClientRequest(command) {
			reply.WrongLeader = false
			reply.Err = "OK"
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// ========== Lock Region ==========
	sm.mu.Lock()
	if args.Num != -1 && args.Num < len(sm.configs) {
		reply.Config = sm.configs[args.Num]
		reply.WrongLeader = false
		reply.Err = "OK"
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	// ========== Lock Region ==========

	reply.WrongLeader = true
	command := Op{
		Operation:         args,
		RequestIdentifier: args.Identifier,
	}

	index, term, is_leader := sm.rf.Start(command)
	if is_leader {
		replyChan := make(chan CommandReply, 1)
		defer close(replyChan)

		// ========== Lock Region ==========
		sm.mu.Lock()
		sm.replyChanMap[index] = replyChan
		sm.mu.Unlock()
		// ========== Lock Region ==========

		// waiting raft executing command
		select {
		case serverReply := <-replyChan:
			reply.Err = "OK"
			reply.Config = serverReply.Config
			reply.WrongLeader = false

			checkTerm, checkLeader := sm.rf.GetState()

			if checkTerm != term || !checkLeader {
				reply.WrongLeader = true
			}
		case <-time.After(300 * time.Millisecond):
			reply.WrongLeader = true
		}

		// ========== Lock Region ==========
		sm.mu.Lock()
		delete(sm.replyChanMap, index)
		sm.mu.Unlock()
		// ========== Lock Region ==========
	}
}

func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	// for communication between servers
	labgob.Register(Op{})
	labgob.Register(&JoinArgs{})
	labgob.Register(&LeaveArgs{})
	labgob.Register(&MoveArgs{})
	labgob.Register(&QueryArgs{})

	sm.commandIndex = 0
	sm.applyCh = make(chan raft.ApplyMsg, 10)
	sm.replyRecord = make(map[int]int)
	sm.replyChanMap = make(map[int]chan CommandReply)
	sm.distribution = make(map[int]*list.List)
	sm.distribution[0] = list.New()
	for i := 0; i < NShards; i++ {
		sm.distribution[0].PushBack(i)
	}

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.commandHandler()

	return sm
}

func (sm *ShardMaster) commandHandler() {
	// Lab 4B Test will not automatically call the Kill function of shardmaster, use noCommandCnt to check if test has ended
	noCommandCnt := 0

	for !sm.killed() {
		select {
		case m := <-sm.applyCh:
			noCommandCnt = 0

			// ========== Lock Region ==========
			sm.mu.Lock()

			command, _ := m.Command.(Op)
			identifier := command.RequestIdentifier

			var reply CommandReply
			if args, ok := command.Operation.(*QueryArgs); ok {
				var config Config
				if args.Num == -1 || args.Num >= len(sm.configs) {
					config = sm.configs[len(sm.configs)-1]
				} else {
					config = sm.configs[args.Num]
				}

				reply.Config = config
			} else {
				lastRequestId, exist := sm.replyRecord[identifier.ClientId]
				if exist && identifier.RequestId == lastRequestId {
					DPrintf("ShardMaster(%d) received repeat command from client(%d) with ClientRequestIndex %d\n", sm.me, identifier.ClientId, identifier.RequestId)
				} else {
					var shards [NShards]int
					lastConfig := sm.configs[len(sm.configs)-1]
					num := lastConfig.Num + 1
					group := make(map[int][]string, len(lastConfig.Groups))
					for key, value := range lastConfig.Groups {
						group[key] = value
					}

					switch args := command.Operation.(type) {
					case *JoinArgs:
						for key, value := range args.Servers {
							group[key] = value
							sm.distribution[key] = list.New()
						}

						if len(sm.distribution) > 1 { // in case of no valid replica
							sm.loadBalance()
						}
						DPrintf("ShardMaster(%d) executed Join command from client(%d) with ClientRequestIndex %d\n", sm.me, identifier.ClientId, identifier.RequestId)
					case *LeaveArgs:
						for _, gid := range args.GIDs {
							for sm.distribution[gid].Len() != 0 {
								moveShard(sm.distribution[0], sm.distribution[gid])
							}

							delete(sm.distribution, gid)
							delete(group, gid)
						}

						if len(sm.distribution) > 1 { // in case of no valid replica
							sm.loadBalance()
						}
						DPrintf("ShardMaster(%d) executed Leave command from client(%d) with ClientRequestIndex %d\n", sm.me, identifier.ClientId, identifier.RequestId)
					case *MoveArgs:
						src := lastConfig.Shards[args.Shard]
						for e := sm.distribution[src].Front(); e != nil; e = e.Next() {
							if e.Value == args.Shard {
								sm.distribution[src].MoveToBack(e)
								break
							}
						}

						moveShard(sm.distribution[args.GID], sm.distribution[src])

						DPrintf("ShardMaster(%d) executed Move command from client(%d) with ClientRequestIndex %d\n", sm.me, identifier.ClientId, identifier.RequestId)
					default:
						DPrintf("Error: unknown type\n")
					}

					// build shards from shard distribution
					for key, value := range sm.distribution {
						for e := value.Front(); e != nil; e = e.Next() {
							index := e.Value.(int)
							shards[index] = key
						}
					}

					sm.configs = append(sm.configs, Config{
						Num:    num,
						Shards: shards,
						Groups: group,
					})
					DPrintf("Generate new configuration: %v\n", sm.configs[len(sm.configs)-1])

					sm.replyRecord[identifier.ClientId] = identifier.RequestId
				}
			}

			sm.commandIndex = m.CommandIndex
			replyChan, ok := sm.replyChanMap[m.CommandIndex]
			if ok {
				replyChan <- reply
			}

			sm.mu.Unlock()
			// ========== Lock Region ==========
		case <-time.After(1000 * time.Millisecond):
			if noCommandCnt < 5 {
				DPrintf("ShardMaster(%d) has been waiting for command for more than one second, noCommandCnt: %d\n", sm.me, noCommandCnt)
				noCommandCnt++
			} else {
				sm.Kill()
			}
		}
	}
}

// move shard from src replica to dst replica
func moveShard(dst *list.List, src *list.List) {
	value := src.Remove(src.Back())
	dst.PushBack(value)
}

// move shard between replicas to reach load-balance
func (sm *ShardMaster) loadBalance() {
	// make each traversal orderly
	var entries []DistributionEntry
	for key, value := range sm.distribution {
		entries = append(entries, DistributionEntry{Gid: key, Replica: value})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Gid < entries[j].Gid
	})

	// compute the balanced load
	balance := NShards / (len(entries) - 1)

	// if there is no valid replica before, move all shards to the replica with the max gid, which will then enter highLoadReplica slice
	// assuming that each gid is greater than 0, it doesn't matter
	if entries[0].Replica.Len() != 0 && len(entries) > 1 {
		for entries[0].Replica.Len() != 0 {
			moveShard(entries[1].Replica, entries[0].Replica)
		}
	}

	lowLoadReplica, highLoadReplica := make([]*list.List, 0), make([]*list.List, 0)
	for _, entry := range entries {
		if entry.Gid == 0 {
			continue
		}

		if entry.Replica.Len() < balance {
			lowLoadReplica = append(lowLoadReplica, entry.Replica)
		} else if entry.Replica.Len() > balance {
			highLoadReplica = append(highLoadReplica, entry.Replica)
		}
	}

	DPrintf("Balance load is %d, len of lowLoadReplica: %d, len of highLoadReplica: %d\n", balance, len(lowLoadReplica), len(highLoadReplica))

	// move shard from highLoadReplica to lowLoadReplica
	for _, list := range lowLoadReplica {
		for i := 0; i < len(highLoadReplica); i++ {
			if list.Len() == balance {
				continue
			}

			for highLoadReplica[i].Len() > balance {
				if list.Len() == balance {
					break
				}

				moveShard(list, highLoadReplica[i])
			}
		}
	}

	// in case of some replica has reach balance but the others is still highLoad
	for _, list := range highLoadReplica {
		if list.Len() == balance || list.Len() == balance+1 {
			continue
		}

		for _, replica := range lowLoadReplica {
			if list.Len() == balance || list.Len() == balance+1 {
				break
			}

			if replica.Len() == balance {
				moveShard(replica, list)
			}
		}
	}
}
