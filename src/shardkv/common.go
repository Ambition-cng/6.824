package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type Identifier struct {
	ClientId  int
	RequestId int
}

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string     // "Put" or "Append"
	Identifier Identifier // Client request identifier
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	Shard       int
	ConfigNum   int
	ShardData   map[string]string
	ReplyRecord map[int]int
}

type MoveShardReply struct {
	Err Err
}
