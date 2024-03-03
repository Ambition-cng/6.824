package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

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

type Record struct {
	RequestId int
	Reply     interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	cond         *sync.Cond
	leaderId     int      // id of leader kvserver
	commandIndex int      // index of highest command known to be executed
	replyRecord  sync.Map // record of previous request, to ensure that the key/value service executes each one just once.
	database     sync.Map // key-value database
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
		val, exist := kv.database.Load(args.Key)
		kv.mu.Unlock()
		// ========== Lock Region ==========

		reply.LeaderId = leaderId
		checkTerm, checkLeader := kv.rf.GetState()
		if checkTerm == term && checkLeader {
			if !exist {
				reply.Err = "ErrNoKey"
				reply.Value = ""
			} else {
				value, _ := val.(string)
				reply.Err = "OK"
				reply.Value = value
			}
		} else {
			reply.Err = "ErrWrongLeader"
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	val, ok := kv.replyRecord.Load(args.Identifier.ClientId)
	if ok {
		record, _ := val.(Record)
		if record.RequestId == args.Identifier.RequestId {
			putAppendReply, _ := record.Reply.(PutAppendReply)
			reply.Err = putAppendReply.Err
			reply.LeaderId = putAppendReply.LeaderId
			return
		}
	}

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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.leaderId = -1
	kv.commandIndex = 0
	kv.database = sync.Map{}
	kv.replyRecord = sync.Map{}
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for m := range kv.applyCh {
			if kv.killed() {
				return
			}

			command, _ := m.Command.(Op)
			identifier := command.RequestIdentifier
			switch command.Operation {
			case "Get":
			case "Put":
				lastRequest, ok := kv.replyRecord.Load(identifier.ClientId)
				if ok {
					record := lastRequest.(Record)
					if record.RequestId == identifier.RequestId {
						continue
					}
				}

				kv.database.Store(command.Key, command.Value)
				kv.replyRecord.Store(identifier.ClientId, Record{
					RequestId: identifier.RequestId,
					Reply: PutAppendReply{
						Err:      "OK",
						LeaderId: m.LeaderId,
					},
				})
				DPrintf("Kvserver(%d) executed %s command(%s-%s) from client(%d) with index %d\n", kv.me, command.Operation, command.Key, command.Value, identifier.ClientId, identifier.RequestId)
			case "Append":
				lastRequest, ok := kv.replyRecord.Load(identifier.ClientId)
				if ok {
					record := lastRequest.(Record)
					if record.RequestId == identifier.RequestId {
						continue
					}
				}

				val, ok := kv.database.Load(command.Key)
				if !ok {
					kv.database.Store(command.Key, command.Value)
				} else {
					value, _ := val.(string)
					kv.database.Store(command.Key, value+command.Value)
				}

				kv.replyRecord.Store(identifier.ClientId, Record{
					RequestId: identifier.RequestId,
					Reply: PutAppendReply{
						Err:      "OK",
						LeaderId: m.LeaderId,
					},
				})
				DPrintf("Kvserver(%d) executed %s command(%s-%s) from client(%d) with index %d\n", kv.me, command.Operation, command.Key, command.Value, identifier.ClientId, identifier.RequestId)
			default:
				DPrintf("Error Command!\n")
			}

			// ========== Lock Region ==========
			kv.mu.Lock()

			kv.leaderId = m.LeaderId
			kv.commandIndex = m.CommandIndex
			kv.cond.Broadcast()

			kv.mu.Unlock()
			// ========== Lock Region ==========
		}
	}()

	return kv
}
