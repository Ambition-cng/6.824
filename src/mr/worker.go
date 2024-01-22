package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	workerState := "idle"
	taskID := -1

	for {
		masterResponse := CallTaskProcess(workerState, taskID)
		logger.Println(masterResponse)

		taskType := masterResponse.TaskType

		if taskType == "map" {
			logger.Println("Processing map task...")
			taskID = masterResponse.TaskID

			filename, NReduce := masterResponse.InputFilePath, masterResponse.FileCount
			intermediates := make([][]KeyValue, NReduce)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			for _, kv := range kva {
				key := ihash(kv.Key) % NReduce
				intermediates[key] = append(intermediates[key], kv)
			}

			for index, intermediate := range intermediates {
				intermediateFilename := "mr-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(index)
				ofile, _ := os.Create(intermediateFilename)

				enc := json.NewEncoder(ofile)

				for _, kv := range intermediate {
					if err := enc.Encode(&kv); err != nil {
						log.Fatalf("Failed to encode kv: %s", err)
					}
				}

				ofile.Close()
			}

			workerState = "mFinished"
			logger.Println("Map task finished.")
		} else if taskType == "reduce" {
			logger.Println("Processing reduce task...")
			taskID = masterResponse.TaskID

			MMap := masterResponse.FileCount
			intermediate := []KeyValue{}

			for index := 0; index < MMap; index++ {
				filename := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(taskID)

				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}

				dec := json.NewDecoder(file)

				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}

				file.Close()
			}

			sort.Sort(ByKey(intermediate))

			oname := "mr-out-" + strconv.Itoa(taskID)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			workerState = "rFinished"
			logger.Println("Reduce task finished.")
		} else if taskType == "echo" {
			time.Sleep(time.Second)
			taskID = -1
			workerState = "idle"
		} else {
			// TaskType == "finish"
			break
		}
	}
}

func CallTaskProcess(state string, taskID int) *MasterResponse {
	logger.Println("Send rpc request to worker")
	req := WorkerRequest{
		WorkerState: state,
		TaskID:      taskID,
	}

	resp := MasterResponse{}

	if call("Master.TaskProcess", &req, &resp) {
		return &resp
	} else {
		return nil
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
