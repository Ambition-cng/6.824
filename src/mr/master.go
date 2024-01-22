package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskInfo struct {
	status int       // 任务的执行状态, 0 - not started, 1 - doing, 2 - done
	timer  time.Time // 任务的执行时间, 任务执行超过10s则重新执行
}

type Master struct {
	// Your definitions here.
	mtx                          sync.Mutex // 互斥锁
	mTaskNum, rTaskNum           int        // M, R
	mTaskFinished, rTaskFinished int        // 记录Map, Reduce任务执行完毕的数量
	inputFiles                   []string   // 输入文件
	mapTaskInfo                  []TaskInfo // 记录所有Map任务的信息
	reduceTaskInfo               []TaskInfo // 记录所有Reduce任务的信息
	stage                        int        // 记录Map-Reduce的执行过程, 0 - DoingMapTasks, 1 - DoingReduceTasks, 2 - AllTasksDone
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) TaskProcess(req *WorkerRequest, resp *MasterResponse) error {
	logger.Printf("Receive rpc request from worker, worker state : %s, taskID : %d\n", req.WorkerState, req.TaskID)
	m.mtx.Lock()
	if req.WorkerState == "mFinished" {
		m.mapTaskInfo[req.TaskID].status = 2
		m.mTaskFinished += 1
		if m.mTaskFinished == m.mTaskNum {
			m.stage = 1
		}
	} else if req.WorkerState == "rFinished" {
		m.reduceTaskInfo[req.TaskID].status = 2
		m.rTaskFinished += 1
		if m.rTaskFinished == m.rTaskNum {
			m.stage = 2
		}
	} else {
		// req.WorkerState == "idle"
		// do nothing
	}

	if m.stage == 0 {
		taskID := -1
		for id, task := range m.mapTaskInfo {
			if task.status == 0 || (task.status == 1 && time.Since(task.timer) > 10*time.Second) {
				m.mapTaskInfo[id] = TaskInfo{
					status: 1,
					timer:  time.Now(),
				}
				taskID = id
				break
			}
		}

		if taskID == -1 {
			resp.TaskType = "echo"
			logger.Println("Waiting for all map tasks done...")
		} else {
			*resp = MasterResponse{
				TaskType:      "map",
				TaskID:        taskID,
				FileCount:     m.rTaskNum,
				InputFilePath: m.inputFiles[taskID],
			}
			logger.Println("Assign worker a map task")
		}
	} else if m.stage == 1 {
		taskID := -1
		for id, task := range m.reduceTaskInfo {
			if task.status == 0 || (task.status == 1 && time.Since(task.timer) > 10*time.Second) {
				m.reduceTaskInfo[id] = TaskInfo{
					status: 1,
					timer:  time.Now(),
				}
				taskID = id
				break
			}
		}

		if taskID == -1 {
			resp.TaskType = "echo"
			logger.Println("Waiting for all reduce tasks done...")
		} else {
			*resp = MasterResponse{
				TaskType:      "reduce",
				TaskID:        taskID,
				FileCount:     m.mTaskNum,
				InputFilePath: "",
			}
			logger.Println("Assign worker a reduce task")
		}
	} else {
		// m.stage == 2
		resp.TaskType = "finish"
	}
	m.mtx.Unlock()

	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mtx.Lock()
	ret := m.stage == 2
	m.mtx.Unlock()

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Initilize master
	m.mTaskNum, m.rTaskNum = len(files), nReduce
	m.inputFiles = files
	m.mapTaskInfo = make([]TaskInfo, m.mTaskNum)
	m.reduceTaskInfo = make([]TaskInfo, m.rTaskNum)

	m.server()
	return &m
}
