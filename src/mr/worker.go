package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerStatus string

const (
	CREATED WorkerStatus = "CREATED"
	IDLE    WorkerStatus = "IDLE"
	BUSY    WorkerStatus = "BUSY"
)

type WorkerState struct {
	Status WorkerStatus
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	ws := &WorkerState{
		Status: "CREATED",
	}
	for {
		switch ws.Status {
		case CREATED:
			// call RPC to register
			ws.CallRegister()
		case IDLE:
			fmt.Println("trying to get task...")
			ws.CallGetTask()
			time.Sleep(2 * time.Second)
		case BUSY:
			fmt.Println("Doing Task...")
			time.Sleep(5 * time.Second)
			ws.Status = "IDLE"
		}
		time.Sleep(2 * time.Second)
	}
}

func (ws *WorkerState) CheckFinishTask() {
	// if finish task, change status to idle
	ws.Status = IDLE
}

func (ws *WorkerState) CallGetTask() {
	// if task
	// ws.Status = BUSY
	// else
	// just sleep 1 sec and
	// continue to poll for tasks
	// get pid of running process
	args := GetTaskArg{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Println("GetTask success")
		if reply.File != "" {
			fmt.Printf("Handling task for target: %s::id: %d\n", reply.File, reply.ID)
			ws.Status = BUSY
			// doing tasks
		} else {
			fmt.Println("All tasks completed")
			os.Exit(0)
		}
	}
}

func (ws *WorkerState) CallRegister() {
	// get pid of running process
	pid := os.Getpid()
	args := RegisterArgs{
		ID: strconv.Itoa(pid),
	}
	reply := RegisterReply{}
	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		fmt.Println("Register success")
		if reply.Status == "success" {
			ws.Status = IDLE
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
