package kvraft

import (
	"crypto/rand"
	"math/big"
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
	leaderId  int
	clientId  int
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.leaderId = 0
	ck.clientId = generateClientIdentifier()
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ret := ""

	for {
		args := &GetArgs{
			Key: key,
		}
		reply := &GetReply{}

		ok := ck.sendRpcRequest("KVServer.Get", args, reply)
		if !ok {
			ck.leaderId = int(nrand()) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == "ErrWrongLeader" {
			if reply.LeaderId == -1 {
				ck.leaderId = int(nrand()) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
			} else {
				ck.leaderId = reply.LeaderId
			}
		} else {
			ret = reply.Value
			break
		}
	}

	return ret
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId += 1

	for {
		args := &PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
			Identifier: Identifier{
				ClientId:  ck.clientId,
				RequestId: ck.requestId,
			},
		}
		reply := &PutAppendReply{}

		DPrintf("Client(%d) - sending PutAppend(%s) RPC to kvserver(%d) with key(%s) and value(%s)\n", ck.clientId, op, ck.leaderId, key, value)
		ok := ck.sendRpcRequest("KVServer.PutAppend", args, reply)
		if !ok {
			DPrintf("Client(%d) - cannot hear from kvserver(%d), switch to next kvserver and retry\n", ck.clientId, ck.leaderId)
			ck.leaderId = int(nrand()) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if reply.Err == "ErrWrongLeader" {
			DPrintf("Client(%d) - kvserver(%d) is not Raft Leader, switch to next kvserver and retry\n", ck.clientId, ck.leaderId)
			if reply.LeaderId == -1 {
				ck.leaderId = int(nrand()) % len(ck.servers)
				time.Sleep(100 * time.Millisecond)
			} else {
				ck.leaderId = reply.LeaderId
			}
		} else {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendRpcRequest(methodName string, args interface{}, reply interface{}) bool {
	done := make(chan bool, 1)

	go func() {
		done <- ck.servers[ck.leaderId].Call(methodName, args, reply)
	}()

	select {
	case result := <-done:
		return result
	case <-time.After(2 * time.Second):
		return false
	}
}
