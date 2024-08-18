package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// NOTE: Need to design a state to capture
// splits -> MMaps -> RREduces
// 6 			-> 10 	 -> 10
// constraint: worker won't be available and will be added gradually
type Task struct {
	Target string
	Status string // AVAILABLE | ACQUIRED | DONE
}

// one Mapper only writes to 1 Mapfile
type TempOutput struct {
	File string
	Kind string
	// just use simple state (string) ACQUIRED | OPEN
	// if race condition happens, use LOCK with timeout
}

type Executor struct {
	ID     string
	Status string
}

type Coordinator struct {
	mapTaskCh chan string
	Workers   []*Executor
	Tasks     []*Task
	NReduce   int
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	fmt.Println("A new worker connected, ID: ", args.ID)
	c.Workers = append(c.Workers, &Executor{
		ID: args.ID,
	})
	reply.Status = "success"
	return nil
}

func (c *Coordinator) dispatchMapTask() {
	for _, task := range c.Tasks {
		if task.Status == "AVAILABLE" {
			c.mapTaskCh <- task.Target
			task.Status = "ACQUIRED"
		}
	}
	close(c.mapTaskCh)
}

func (c *Coordinator) GetTask(arg *GetTaskArg, reply *GetTaskReply) error {
	taskTarget := <-c.mapTaskCh
	if taskTarget != "" {
		reply.File = taskTarget
	} else {
		reply.File = ""
	}
	reply.NReduce = c.NReduce
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	fmt.Println("RPC server listening...")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]*Task, len(files))
	for idx, file := range files {
		tasks[idx] = &Task{
			Status: "AVAILABLE",
			Target: file,
		}
	}
	c := Coordinator{
		Workers:   make([]*Executor, 0),
		Tasks:     tasks,
		mapTaskCh: make(chan string),
		NReduce:   nReduce,
	}
	go c.dispatchMapTask()
	c.server()
	return &c
}
