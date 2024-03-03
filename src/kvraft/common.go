package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
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
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}
