package shardmaster

//
// Shardmaster clerk.
//

import (
	"sync/atomic"
	"time"

	"../labrpc"
)

var clientIdentifier int64

func generateClientIdentifier() int {
	atomic.AddInt64(&clientIdentifier, 1)
	return int(clientIdentifier)
}

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int
	requestId int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.clientId = generateClientIdentifier()
	ck.requestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num

	DPrintf("Client(%d) sending Query command with num %d\n", ck.clientId, num)
	for {
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	ck.requestId += 1
	args.Identifier = Identifier{
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	args.Servers = servers

	DPrintf("Client(%d) sending Join command(requestId: %d), joined servers: %v\n", ck.clientId, ck.requestId, servers)
	for {
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}

	ck.requestId += 1
	args.Identifier = Identifier{
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	args.GIDs = gids

	DPrintf("Client(%d) sending Leave command(requestId: %d), leaved servers: %v\n", ck.clientId, ck.requestId, gids)
	for {
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}

	ck.requestId += 1
	args.Identifier = Identifier{
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	args.Shard = shard
	args.GID = gid

	DPrintf("Client(%d) sending Move command(requestId: %d), move shard %d to server %d\n", ck.clientId, ck.requestId, shard, gid)
	for {
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
