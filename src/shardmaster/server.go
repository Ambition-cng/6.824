package shardmaster

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	cond         *sync.Cond
	commandIndex int         // index of highest command known to be executed
	replyRecord  map[int]int // record of previous request, to ensure that ShardMaster executes each request just once.
	configs      []Config    // indexed by config num

	distribution map[int]*list.List // distribution of shards
}

type Op struct {
	Operation         interface{}
	RequestIdentifier Identifier
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
	index, term, is_leader := sm.rf.Start(command)
	if is_leader {
		// ========== Lock Region ==========
		sm.mu.Lock()
		for sm.commandIndex < index {
			sm.cond.Wait()
		}
		sm.mu.Unlock()
		// ========== Lock Region ==========

		checkTerm, checkLeader := sm.rf.GetState()
		if checkTerm == term && checkLeader {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if sm.checkRepeatRequest(args.Identifier) {
		reply.Err = "OK"
		return
	}

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

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sm.checkRepeatRequest(args.Identifier) {
		reply.Err = "OK"
		return
	}

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

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if sm.checkRepeatRequest(args.Identifier) {
		reply.Err = "OK"
		return
	}

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

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	reply.WrongLeader = true
	command := Op{
		Operation:         args,
		RequestIdentifier: args.Identifier,
	}

	index, term, is_leader := sm.rf.Start(command)
	if is_leader {
		// ========== Lock Region ==========
		sm.mu.Lock()
		for sm.commandIndex < index {
			sm.cond.Wait()
		}

		var config Config
		if args.Num == -1 || args.Num >= len(sm.configs) {
			config = sm.configs[len(sm.configs)-1]
		} else {
			config = sm.configs[args.Num]
		}
		sm.mu.Unlock()
		// ========== Lock Region ==========

		checkTerm, checkLeader := sm.rf.GetState()
		if checkTerm == term && checkLeader {
			reply.Config = config
			reply.WrongLeader = false
			reply.Err = "OK"
		}
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
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.cond = sync.NewCond(&sm.mu)
	sm.replyRecord = make(map[int]int)
	sm.distribution = make(map[int]*list.List)
	sm.distribution[0] = list.New()
	for i := 0; i < NShards; i++ {
		sm.distribution[0].PushBack(i)
	}

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go func() {
		for m := range sm.applyCh {
			if sm.killed() {
				return
			}

			command, _ := m.Command.(Op)
			identifier := command.RequestIdentifier

			if _, ok := command.Operation.(*QueryArgs); ok {
				// ========== Lock Region ==========
				sm.mu.Lock()

				sm.commandIndex = m.CommandIndex
				sm.cond.Broadcast()

				sm.mu.Unlock()
				// ========== Lock Region ==========
			} else {
				// ========== Lock Region ==========
				sm.mu.Lock()

				lastRequestId, ok := sm.replyRecord[identifier.ClientId]
				if ok {
					if identifier.RequestId == lastRequestId {
						sm.commandIndex = m.CommandIndex
						sm.cond.Broadcast()
						sm.mu.Unlock()
						continue
					}
				}

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

				sm.commandIndex = m.CommandIndex
				sm.replyRecord[identifier.ClientId] = identifier.RequestId
				sm.cond.Broadcast()

				sm.mu.Unlock()
				// ========== Lock Region ==========
			}
		}
	}()

	return sm
}

// move shard from src replica to dst replica
func moveShard(dst *list.List, src *list.List) {
	value := src.Remove(src.Back())
	dst.PushBack(value)
}

// move shard between replicas to reach load-balance
func (sm *ShardMaster) loadBalance() {
	// compute the balanced load
	balance := NShards / (len(sm.distribution) - 1)

	// if there is no valid replica before, move all shards to a random replica, which will enter highLoadReplica slice
	if sm.distribution[0].Len() != 0 {
		for key := range sm.distribution {
			if key != 0 {
				for sm.distribution[0].Len() != 0 {
					moveShard(sm.distribution[key], sm.distribution[0])
				}
				break
			}
		}
	}

	lowLoadReplica, highLoadReplica := make([]*list.List, 0), make([]*list.List, 0)
	for key, value := range sm.distribution {
		if key == 0 {
			continue
		}

		if value.Len() < balance {
			lowLoadReplica = append(lowLoadReplica, value)
		} else if value.Len() > balance {
			highLoadReplica = append(highLoadReplica, value)
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
