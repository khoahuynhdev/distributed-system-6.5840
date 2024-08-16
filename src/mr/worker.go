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
			time.Sleep(2 * time.Second)
		case BUSY:
		}
		time.Sleep(2 * time.Second)
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
