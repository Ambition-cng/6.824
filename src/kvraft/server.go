package kvraft

import (
	"log"
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

type Op struct {
	Operation         string
	Key               string
	Value             string
	RequestIdentifier Identifier
}

type ServerSnapshot struct {
	Database    map[string]string // state machine state
	ReplyRecord map[int]int       // client request record
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	cond         *sync.Cond
	leaderId     int               // id of leader kvserver
	commandIndex int               // index of highest command known to be executed
	replyRecord  map[int]int       // record of previous request, to ensure that the key/value service executes each one just once.
	database     map[string]string // key-value database
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err = "ErrWrongLeader"
	reply.Value = ""
	reply.LeaderId = -1

	command := Op{
		Operation: "Get",
		Key:       args.Key,
		Value:     "",
	}

	index, term, is_leader := kv.rf.Start(command)
	if is_leader {
		// ========== Lock Region ==========
		kv.mu.Lock()
		for kv.commandIndex < index {
			kv.cond.Wait()
		}

		leaderId := kv.leaderId
		value, exist := kv.database[args.Key]
		kv.mu.Unlock()
		// ========== Lock Region ==========

		reply.LeaderId = leaderId
		checkTerm, checkLeader := kv.rf.GetState()
		if checkTerm == term && checkLeader {
			if !exist {
				reply.Err = "ErrNoKey"
				reply.Value = ""
			} else {
				reply.Err = "OK"
				reply.Value = value
			}
		} else {
			reply.Err = "ErrWrongLeader"
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// ========== Lock Region ==========
	kv.mu.Lock()
	requestId, ok := kv.replyRecord[args.Identifier.ClientId]
	if ok {
		if args.Identifier.RequestId == requestId {
			reply.Err = "OK"
			kv.mu.Unlock()
			return
		}
		DPrintf("KvServer(%d) replyRecord of Client(%d) is %d, get new request %d\n", kv.me, args.Identifier.ClientId, requestId, args.Identifier.RequestId)
	}
	kv.mu.Unlock()
	// ========== Lock Region ==========

	reply.Err = "ErrWrongLeader"
	reply.LeaderId = -1

	command := Op{
		Operation:         args.Op,
		Key:               args.Key,
		Value:             args.Value,
		RequestIdentifier: args.Identifier,
	}

	index, term, is_leader := kv.rf.Start(command)
	if is_leader {
		DPrintf("Kvserver(%d) got PutAppend commmand(%s-%s) from client(%d) with index %d in term %d\n", kv.me, args.Key, args.Value, args.Identifier.ClientId, index, term)
		// ========== Lock Region ==========
		kv.mu.Lock()
		for kv.commandIndex < index {
			kv.cond.Wait()
		}

		leaderId := kv.leaderId
		kv.mu.Unlock()
		// ========== Lock Region ==========

		reply.LeaderId = leaderId
		checkTerm, checkLeader := kv.rf.GetState()
		if checkTerm == term && checkLeader {
			reply.Err = "OK"
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ServerSnapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.leaderId = -1
	kv.commandIndex = 0
	kv.database = make(map[string]string)
	kv.replyRecord = make(map[int]int)
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)

	go kv.commandHandler()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.logCompaction(persister)

	return kv
}

func (kv *KVServer) commandHandler() {
	for m := range kv.applyCh {
		if kv.killed() {
			return
		}

		if m.Snapshot == nil {
			command, _ := m.Command.(Op)
			identifier := command.RequestIdentifier
			if command.Operation == "Get" {
				// ========== Lock Region ==========
				kv.mu.Lock()

				kv.leaderId = m.LeaderId
				kv.commandIndex = m.CommandIndex
				kv.cond.Broadcast()

				kv.mu.Unlock()
				// ========== Lock Region ==========
			} else {
				// ========== Lock Region ==========
				kv.mu.Lock()
				lastRequestId, ok := kv.replyRecord[identifier.ClientId]
				if ok {
					if identifier.RequestId == lastRequestId {
						kv.leaderId = m.LeaderId
						kv.commandIndex = m.CommandIndex
						kv.cond.Broadcast()
						kv.mu.Unlock()
						continue
					}
				}

				if command.Operation == "Put" {
					kv.database[command.Key] = command.Value
				} else {
					value, ok := kv.database[command.Key]
					if !ok {
						kv.database[command.Key] = command.Value
					} else {
						kv.database[command.Key] = value + command.Value
					}
				}
				DPrintf("Kvserver(%d) executed %s command(%s-%s) from client(%d) with ClientRequestIndex %d\n", kv.me, command.Operation, command.Key, command.Value, identifier.ClientId, identifier.RequestId)

				kv.leaderId = m.LeaderId
				kv.commandIndex = m.CommandIndex
				kv.cond.Broadcast()

				kv.replyRecord[identifier.ClientId] = identifier.RequestId

				kv.mu.Unlock()
				// ========== Lock Region ==========
			}
		} else {
			// ========== Lock Region ==========
			kv.mu.Lock()
			DPrintf("KvServer(%d) reconstruct from snapshot with index %d\n", kv.me, m.Snapshot.LastIncludedIndex)

			kv.leaderId = m.LeaderId
			kv.commandIndex = m.Snapshot.LastIncludedIndex

			serverSnapshot := m.Snapshot.ServerSnapshot.(ServerSnapshot)

			kv.database = make(map[string]string)
			for key, value := range serverSnapshot.Database {
				kv.database[key] = value
			}

			kv.replyRecord = make(map[int]int)
			for key, value := range serverSnapshot.ReplyRecord {
				kv.replyRecord[key] = value
			}

			kv.mu.Unlock()
			// ========== Lock Region ==========
		}
	}
}

func (kv *KVServer) logCompaction(persister *raft.Persister) {
	if kv.maxraftstate == -1 {
		return
	}

	for {
		if kv.killed() {
			return
		}

		raftStateSize := persister.RaftStateSize()

		if raftStateSize < kv.maxraftstate {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		// ========== Lock Region ==========
		kv.mu.Lock()
		DPrintf("KvServer(%d) RaftStateSize(%d) over limit(%d), generate snapshot with index %d\n", kv.me, raftStateSize, kv.maxraftstate, kv.commandIndex)

		copiedDatabase := make(map[string]string)
		for key, value := range kv.database {
			copiedDatabase[key] = value
		}

		copiedReplyRecord := make(map[int]int)
		for key, value := range kv.replyRecord {
			copiedReplyRecord[key] = value
		}

		serverSnapshot := ServerSnapshot{
			Database:    copiedDatabase,
			ReplyRecord: copiedReplyRecord,
		}

		snapshot := raft.Snapshot{
			LastIncludedIndex: kv.commandIndex,
			ServerSnapshot:    serverSnapshot,
		}

		kv.mu.Unlock()
		// ========== Lock Region ==========

		kv.rf.Start(snapshot)

		time.Sleep(50 * time.Millisecond)
	}
}
