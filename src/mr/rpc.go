package mr

import (
	"os"
	"strconv"
)

type WorkerRequest struct {
	WorkerState string // idle, mFinished, rFinished
	TaskID      int
	// idle: 等待被分配任务, TaskID == -1
	// mFinished: 处理完成Map任务, 生成R个中间文件(intermediate/mr-TaskID-0,1,2,...,R-1)
	// rFinished: 处理完成Reduce任务, 生成1个结果文件(mr-out-TaskID)
}

type MasterResponse struct {
	TaskType          string // echo, map, reduce, finish
	TaskID, FileCount int
	InputFilePath     string
	// echo: 暂无任务分配
	// map: 分配第TaskID个Map任务给Worker, TaskID取值为0到M-1, FileCount == R,InputFilePath == pr-*.txt中的一个
	// reduce: 分配第TaskID个Map任务给Worker, TaskID取值为0到R-1, FileCount == M,InputFilePath == intermediate/mr-*-TaskID, Reduce-Worker需要遍历所有满足以上格式的文件(M个)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
