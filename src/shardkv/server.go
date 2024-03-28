package shardkv

import (
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"../shardmaster"

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
	Operation         interface{}
	Shard             int
	RequestIdentifier Identifier
}

type DeleteData struct {
	Shard     int
	ConfigNum int
}

type CommandReply struct {
	Err   Err
	Value string
}

type ServerSnapshot struct {
	Database       map[string]string // state machine state
	ReplyRecord    map[int]int       // client request record
	ShardStatus    [shardmaster.NShards]string
	CurrentConfig  shardmaster.Config
	PreviousConfig shardmaster.Config
}

type MigrantData struct {
	Shard       int
	ConfigNum   int
	ShardData   map[string]string
	ReplyRecord map[int]int
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	mck *shardmaster.Clerk // used to talk to the shardmaster

	commandIndex int                         // index of highest command known to be executed
	replyChanMap map[int]chan CommandReply   // used to receive replies for Get or PutAppend commands, key - commandIndex, value - channel of reply
	replyRecord  map[int]int                 // record of previous request, to ensure that the key/value service executes each one just once.
	database     map[string]string           // key-value database
	shardStatus  [shardmaster.NShards]string // record of shard status: Idle(no operation), Pending(migranting data), Waiting(waiting for shard data), Serving(ready to respond to client requests)

	curConfig  shardmaster.Config // current config, being processed
	prevConfig shardmaster.Config // previous config of curConfig
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// ========== Lock Region ==========
	kv.mu.Lock()
	shard := key2shard(args.Key)
	gid, status := kv.curConfig.Shards[shard], kv.shardStatus[shard]
	kv.mu.Unlock()
	// ========== Lock Region ==========

	DPrintf("KvServer(%d-%d) receiving a Get command from Client, with Key-%s, Shard-%d\n", kv.gid, kv.me, args.Key, shard)
	if gid != kv.gid || status != "Serving" { // waiting for shard data or not responsible for this shard
		reply.Err = "ErrWrongLeader"
	} else {
		command := Op{
			Operation: args,
		}

		reply.Err = "ErrWrongLeader"

		index, term, is_leader := kv.rf.Start(command)
		if is_leader {
			// using buffered channel to prevent sender from blocking
			replyChan := make(chan CommandReply, 1)
			defer close(replyChan)

			// ========== Lock Region ==========
			kv.mu.Lock()
			kv.replyChanMap[index] = replyChan
			kv.mu.Unlock()
			// ========== Lock Region ==========

			// waiting raft executing command
			select {
			case serverReply := <-replyChan:
				reply.Err = serverReply.Err
				reply.Value = serverReply.Value

				checkTerm, checkLeader := kv.rf.GetState()

				if checkTerm != term || !checkLeader {
					reply.Err = "ErrWrongLeader"
				}
			case <-time.After(300 * time.Millisecond):
				reply.Err = "ErrWrongLeader"
			}

			// ========== Lock Region ==========
			kv.mu.Lock()
			delete(kv.replyChanMap, index)
			kv.mu.Unlock()
			// ========== Lock Region ==========
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// ========== Lock Region ==========
	kv.mu.Lock()
	shard := key2shard(args.Key)
	requestId, ok := kv.replyRecord[args.Identifier.ClientId]
	gid, status := kv.curConfig.Shards[shard], kv.shardStatus[shard]
	kv.mu.Unlock()
	// ========== Lock Region ==========

	DPrintf("KvServer(%d-%d) receiving a PutAppend command from Client(%d), with Key-%s, Value-%s, Shard-%d\n", kv.gid, kv.me, args.Identifier.ClientId, args.Key, args.Value, shard)
	if ok && args.Identifier.RequestId == requestId { // repeat command
		reply.Err = "OK"
	} else if gid != kv.gid || status != "Serving" { // waiting for shard data or not responsible for this shard
		reply.Err = "ErrWrongLeader"
	} else {
		command := Op{
			Operation:         args,
			Shard:             shard,
			RequestIdentifier: args.Identifier,
		}

		reply.Err = "ErrWrongLeader"

		index, term, is_leader := kv.rf.Start(command)
		if is_leader {
			DPrintf("Kvserver(%d-%d) got PutAppend commmand(%s-%s) from client(%d) with index %d in term %d\n", kv.gid, kv.me, args.Key, args.Value, args.Identifier.ClientId, index, term)

			// using buffered channel to prevent sender from blocking
			replyChan := make(chan CommandReply, 1)
			defer close(replyChan)

			// ========== Lock Region ==========
			kv.mu.Lock()
			kv.replyChanMap[index] = replyChan
			kv.mu.Unlock()
			// ========== Lock Region ==========

			// waiting raft executing command
			select {
			case serverReply := <-replyChan:
				reply.Err = serverReply.Err

				checkTerm, checkLeader := kv.rf.GetState()

				if checkTerm != term || !checkLeader {
					reply.Err = "ErrWrongLeader"
				}
			case <-time.After(300 * time.Millisecond):
				reply.Err = "ErrWrongLeader"
			}

			// ========== Lock Region ==========
			kv.mu.Lock()
			delete(kv.replyChanMap, index)
			kv.mu.Unlock()
			// ========== Lock Region ==========
		}

		DPrintf("Kvserver(%d-%d) response PutAppend commmand(%s-%s) from client(%d) with %s\n", kv.gid, kv.me, args.Key, args.Value, args.Identifier.ClientId, reply.Err)
	}
}

/*
Migrant data: when config changes, the ownership of Shard N is transferred from Group A to Group B, there are two methods - Push or Pull data:
 1. Push: Group A push data to Group B, periodicaly check status == Pending; sending migrant data; waiting for Group B's config catching up and turn to Serving status.
 2. Pull: Group B pull data from Group A, periodicaly check status == Waiting; sending migrant request to Group A; waiting for Group A's config catching up and send shard data back; apply shard data by Raft.

However, if using Pull method, an additional response is required in order for Group A to know whether to deleta Shard data.
*/
func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) { // Push method
	DPrintf("KvServer(%d-%d) received MoveShard request of shard %d\n", kv.gid, kv.me, args.Shard)

	// ========== Lock Region ==========
	kv.mu.Lock()
	neededConfigNum := kv.prevConfig.Num
	shardInfo := kv.shardStatus[args.Shard]
	kv.mu.Unlock()
	// ========== Lock Region ==========

	if neededConfigNum < args.ConfigNum {
		DPrintf("KvServer(%d-%d)'s config is lagging behind, configNum: %d, request config number: %d\n", kv.gid, kv.me, neededConfigNum, args.ConfigNum)
		reply.Err = "ErrWrongLeader"
	} else if neededConfigNum == args.ConfigNum {
		if shardInfo != "Waiting" {
			reply.Err = "OK"
		} else {
			DPrintf("KvServer(%d-%d) received data for shard %d, with configNum: %d\n", kv.gid, kv.me, args.Shard, args.ConfigNum)
			command := Op{
				Operation: MigrantData{
					Shard:       args.Shard,
					ConfigNum:   args.ConfigNum,
					ShardData:   args.ShardData,
					ReplyRecord: args.ReplyRecord,
				},
			}
			kv.rf.Start(command)
			reply.Err = "ErrWrongLeader"
		}
	} else { // neededConfigNum > args.ConfigNum, data has already successfully migranted
		reply.Err = "OK"
	}

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// Register data structure for communication
	labgob.Register(Op{})
	labgob.Register(&GetArgs{})
	labgob.Register(&PutAppendArgs{})
	labgob.Register(MigrantData{})
	labgob.Register(DeleteData{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(ServerSnapshot{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.commandIndex = 0
	kv.database = make(map[string]string)
	kv.replyRecord = make(map[int]int)
	kv.replyChanMap = make(map[int]chan CommandReply)

	for index := 0; index < shardmaster.NShards; index++ {
		kv.shardStatus[index] = "Idle"
	}

	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 10)

	go kv.commandHandler()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.logCompaction(persister)
	go kv.fetchLatestConfiguration()
	go kv.checkShardStatus()

	return kv
}

/*
All server state changes are made through Raft to achieve consistency.

	CommandHandler is used to receive all commands generated by Raft.
	To ensure performance and prevent deadlocks, all operations in CommandHandler are non-blocking.
*/
func (kv *ShardKV) commandHandler() {
	for !kv.killed() {
		// use select-case to prevent goroutine from continuouly blocking and periodically check exit conditions
		select {
		case m := <-kv.applyCh:
			// ========== Lock Region ==========
			kv.mu.Lock()

			if m.Snapshot == nil {
				command, _ := m.Command.(Op)
				identifier := command.RequestIdentifier

				switch operation := command.Operation.(type) {
				case *GetArgs:
					value, exist := kv.database[operation.Key]
					shard := key2shard(operation.Key)

					var reply CommandReply
					if kv.curConfig.Shards[shard] != kv.gid {
						reply.Err = "ErrWrongGroup"
					} else {
						if kv.shardStatus[shard] == "Serving" {
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

					replyChan, ok := kv.replyChanMap[m.CommandIndex]
					if ok {
						replyChan <- reply
					}
				case *PutAppendArgs:
					lastRequestId, exist := kv.replyRecord[identifier.ClientId]
					shard := key2shard(operation.Key)

					var reply CommandReply
					if exist && identifier.RequestId == lastRequestId {
						reply.Err = "OK"
						DPrintf("Kvserver(%d-%d) duplicate %s command(%s-%s) from client(%d) with ClientRequestIndex %d\n", kv.gid, kv.me, operation.Op, operation.Key, operation.Value, identifier.ClientId, identifier.RequestId)
					} else if kv.curConfig.Shards[shard] != kv.gid || kv.shardStatus[shard] != "Serving" {
						reply.Err = "ErrWrongGroup"
						DPrintf("Kvserver(%d-%d) mismatch %s command(%s-%s) from client(%d) with ClientRequestIndex %d\n", kv.gid, kv.me, operation.Op, operation.Key, operation.Value, identifier.ClientId, identifier.RequestId)
					} else {
						if operation.Op == "Put" {
							kv.database[operation.Key] = operation.Value
						} else {
							value, ok := kv.database[operation.Key]
							if !ok {
								kv.database[operation.Key] = operation.Value
							} else {
								kv.database[operation.Key] = value + operation.Value
							}
						}

						reply.Err = "OK"
						DPrintf("Kvserver(%d-%d) executed %s command(%s-%s) from client(%d) with ClientRequestIndex %d\n", kv.gid, kv.me, operation.Op, operation.Key, operation.Value, identifier.ClientId, identifier.RequestId)
						kv.replyRecord[identifier.ClientId] = identifier.RequestId
					}

					replyChan, ok := kv.replyChanMap[m.CommandIndex]
					if ok {
						replyChan <- reply
					}

				case shardmaster.Config:
					// Obey the rule: Process re-configurations one at a time, in order.
					if operation.Num == kv.curConfig.Num+1 {
						// if there is a shard whose status is Waiting or Pending, new configuration cannot be updated
						isMigratingData := false
						for index := 0; index < shardmaster.NShards; index++ {
							if kv.shardStatus[index] == "Waiting" || kv.shardStatus[index] == "Pending" {
								isMigratingData = true
								break
							}
						}

						if isMigratingData {
							DPrintf("KvServer(%d-%d) still has some shard being migrated, shard status %v\n", kv.gid, kv.me, kv.shardStatus)
						} else {
							kv.prevConfig = kv.curConfig
							kv.curConfig = operation

							for index := 0; index < shardmaster.NShards; index++ {
								prevGid := kv.prevConfig.Shards[index]
								if kv.curConfig.Shards[index] == kv.gid {
									if prevGid != 0 && prevGid != kv.gid {
										kv.shardStatus[index] = "Waiting"
									} else {
										kv.shardStatus[index] = "Serving"
									}
								} else {
									if prevGid == kv.gid {
										kv.shardStatus[index] = "Pending"
									} else {
										kv.shardStatus[index] = "Idle"
									}
								}

								DPrintf("KvServer(%d-%d) set shard %d status to %s\n", kv.gid, kv.me, index, kv.shardStatus[index])
							}
							DPrintf("KvServer(%d-%d) config changed, prev config %v,  cur config %v\n", kv.gid, kv.me, kv.prevConfig, kv.curConfig)
						}
					} else {
						DPrintf("KvServre(%d-%d) repeat config or invalid config, current config num: %d, operation config num: %d\n", kv.gid, kv.me, kv.curConfig.Num, operation.Num)
					}
				case MigrantData:
					if kv.shardStatus[operation.Shard] == "Waiting" && operation.ConfigNum == kv.prevConfig.Num {
						DPrintf("Kvserver(%d-%d) successfully copied data for shard %d for config %d from server(%v)\n", kv.gid, kv.me, operation.Shard, kv.prevConfig.Num, kv.prevConfig.Groups[kv.prevConfig.Shards[operation.Shard]])
						for key, value := range operation.ShardData {
							kv.database[key] = value
						}

						for key, value := range operation.ReplyRecord {
							if kv.replyRecord[key] < value {
								kv.replyRecord[key] = value
							}
						}

						kv.shardStatus[operation.Shard] = "Serving"
					}
				case DeleteData:
					if kv.shardStatus[operation.Shard] == "Pending" && operation.ConfigNum == kv.prevConfig.Num {
						DPrintf("Kvserver(%d-%d) delete data of shard %d for config %d\n", kv.gid, kv.me, operation.Shard, operation.ConfigNum)

						for key := range kv.database {
							if key2shard(key) == operation.Shard {
								delete(kv.database, key)
							}
						}

						kv.shardStatus[operation.Shard] = "Idle"
					}
				default:
					DPrintf("Error: unknown type %T\n", operation)
				}

				kv.commandIndex = m.CommandIndex
			} else {
				DPrintf("KvServer(%d-%d) reconstruct from snapshot with index %d\n", kv.gid, kv.me, m.Snapshot.LastIncludedIndex)

				serverSnapshot := m.Snapshot.ServerSnapshot.(ServerSnapshot)

				kv.database = make(map[string]string)
				for key, value := range serverSnapshot.Database {
					kv.database[key] = value
				}

				kv.replyRecord = make(map[int]int)
				for key, value := range serverSnapshot.ReplyRecord {
					kv.replyRecord[key] = value
				}

				kv.shardStatus = serverSnapshot.ShardStatus
				kv.curConfig = serverSnapshot.CurrentConfig
				kv.prevConfig = serverSnapshot.PreviousConfig

				kv.commandIndex = m.Snapshot.LastIncludedIndex
			}

			kv.mu.Unlock()
			// ========== Lock Region ==========
		case <-time.After(1000 * time.Millisecond):
			DPrintf("KvServer(%d-%d) current number of goroutines: %d\n", kv.gid, kv.me, runtime.NumGoroutine())
			DPrintf("KvServer(%d-%d) has been waiting for command for more than one second\n", kv.gid, kv.me)
		}
	}
}

func (kv *ShardKV) logCompaction(persister *raft.Persister) {
	if kv.maxraftstate == -1 {
		return
	}

	for {
		if kv.killed() {
			return
		}

		raftStateSize := persister.RaftStateSize()

		if raftStateSize < kv.maxraftstate {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// ========== Lock Region ==========
		kv.mu.Lock()
		DPrintf("KvServer(%d-%d) RaftStateSize(%d) over limit(%d), generate snapshot with index %d\n", kv.gid, kv.me, raftStateSize, kv.maxraftstate, kv.commandIndex)

		copiedDatabase := make(map[string]string)
		for key, value := range kv.database {
			copiedDatabase[key] = value
		}

		copiedReplyRecord := make(map[int]int)
		for key, value := range kv.replyRecord {
			copiedReplyRecord[key] = value
		}

		serverSnapshot := ServerSnapshot{
			Database:       copiedDatabase,
			ReplyRecord:    copiedReplyRecord,
			ShardStatus:    kv.shardStatus,
			CurrentConfig:  kv.curConfig,
			PreviousConfig: kv.prevConfig,
		}

		snapshot := raft.Snapshot{
			LastIncludedIndex: kv.commandIndex,
			ServerSnapshot:    serverSnapshot,
		}

		kv.mu.Unlock()
		// ========== Lock Region ==========

		kv.rf.Start(snapshot)

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) fetchLatestConfiguration() {
	for {
		if kv.killed() {
			return
		}

		_, is_leader := kv.rf.GetState()
		if is_leader {
			latestConfig := kv.mck.Query(-1)

			// ========== Lock Region ==========
			kv.mu.Lock()
			currentConfigNum := kv.curConfig.Num
			kv.mu.Unlock()
			// ========== Lock Region ==========

			if latestConfig.Num > currentConfigNum { // config changed
				// apply new configurations one by one
				for index := currentConfigNum + 1; index < latestConfig.Num; index++ {
					config := kv.mck.Query(index)
					command := Op{
						Operation: config,
					}
					kv.rf.Start(command)
				}

				command := Op{
					Operation: latestConfig,
				}
				kv.rf.Start(command)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pushShardToReplica(shard int, configNum int, servers []string, shardData map[string]string, replyRecord map[int]int) {
	args := MoveShardArgs{
		Shard:       shard,
		ConfigNum:   configNum,
		ShardData:   shardData,
		ReplyRecord: replyRecord,
	}

	for si := 0; si < len(servers); si++ {
		srv := kv.make_end(servers[si])
		var reply MoveShardReply

		DPrintf("KvServer(%d-%d) sending MoveShard RPC to %s with shard %d\n", kv.gid, kv.me, servers[si], shard)
		ok := srv.Call("ShardKV.MoveShard", &args, &reply)
		if ok && reply.Err == "OK" {
			DPrintf("KvServer(%d-%d) successfully pushed data to %s with shard %d\n", kv.gid, kv.me, servers[si], shard)
			command := Op{
				Operation: DeleteData{
					Shard:     shard,
					ConfigNum: configNum,
				},
			}
			kv.rf.Start(command)
			return
		}
		// ... not ok, or ErrWrongLeader
	}
}

func (kv *ShardKV) checkShardStatus() {
	for {
		if kv.killed() {
			return
		}

		var wg sync.WaitGroup
		// ========== Lock Region ==========
		kv.mu.Lock()

		copiedReplyRecord := make(map[int]int)
		for key, value := range kv.replyRecord {
			copiedReplyRecord[key] = value
		}

		for index := 0; index < shardmaster.NShards; index++ {
			prevGid := kv.prevConfig.Shards[index]
			if prevGid == kv.gid && kv.curConfig.Shards[index] != kv.gid && kv.shardStatus[index] == "Pending" {
				copiedShard := make(map[string]string)
				for key, value := range kv.database {
					if key2shard(key) == index {
						copiedShard[key] = value
					}
				}

				wg.Add(1)
				go func(shard int, configNum int, servers []string, shardData map[string]string, replyRecord map[int]int) {
					defer wg.Done()
					kv.pushShardToReplica(shard, configNum, servers, shardData, replyRecord)
				}(index, kv.prevConfig.Num, kv.curConfig.Groups[kv.curConfig.Shards[index]], copiedShard, copiedReplyRecord)
			}
		}

		kv.mu.Unlock()
		// ========== Lock Region ==========
		wg.Wait()

		time.Sleep(50 * time.Millisecond)
	}
}
