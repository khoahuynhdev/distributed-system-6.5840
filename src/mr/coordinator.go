package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// NOTE: Need to design a state to capture
// splits -> MMaps -> RREduces
// 6 			-> 10 	 -> 10
// constraint: worker won't be available and will be added gradually
type Task struct {
	Kind   string // MAP | REDUCE | WAIT | EXIT
	Target string
	Status string // AVAILABLE | ACQUIRED | DONE
	Id     int
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
	mapTaskCh    chan *Task
	reduceTaskCh chan *Task
	Phase        string
	Workers      []*Executor
	Tasks        []*Task
	NReduce      int
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
	fmt.Println("Dispatching Map task")
	// TODO: assuming worker cannot crash and when a task is dispatch, it will be done successfully
	// worker is not reporting to Coordinator
	// TODO: implement reporting so the task is pushback to Coordinator when a worker crash
	for _, task := range c.Tasks {
		if task.Status == "AVAILABLE" {
			c.mapTaskCh <- task
			task.Status = "ACQUIRED"
		}
	}
	close(c.mapTaskCh)
	// when all the workers finish map task
	// change the phase to REDUCE phase
	// TODO: this should only be changed when all map tasks are finished
	c.Phase = "REDUCE"
}

func (c *Coordinator) dispatchReduceTask() {
	// NOTE: only distribute task when phase changes to REDUCE
	for c.Phase != "REDUCE" {
		time.Sleep(1 * time.Second)
	}
	fmt.Println("Start dispatching reduce task")
	for i := 0; i < c.NReduce; i++ {
		task := fmt.Sprintf("mr-*-%d", i)
		c.reduceTaskCh <- &Task{Target: task, Kind: "REDUCE"}
	}
	close(c.reduceTaskCh)
	// TODO: this should only be changed when all map tasks are finished
	c.Phase = "COMPLETE"
}

func (c *Coordinator) GetTask(arg *GetTaskArg, reply *GetTaskReply) error {
	fmt.Println("Worker polling for task, phase: ", c.Phase)
	switch c.Phase {
	case "MAP":
		taskTarget := <-c.mapTaskCh
		if taskTarget != nil {
			reply.File = taskTarget.Target
			reply.ID = taskTarget.Id
			reply.Kind = "MAP"
		} else {
			reply.File = ""
			reply.ID = -1
		}
	case "REDUCE":
		reduceTarget := <-c.reduceTaskCh
		fmt.Println("Send reduce task ", reduceTarget)
		if reduceTarget != nil {
			reply.File = reduceTarget.Target
			reply.ID = reduceTarget.Id
			reply.Kind = "REDUCE"
		} else {
			reply.File = ""
			reply.ID = -1
		}
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
	return c.Phase == "COMPLETE"
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
			Id:     idx,
			Kind:   "MAP",
		}
	}
	c := Coordinator{
		Workers:      make([]*Executor, 0),
		Tasks:        tasks,
		mapTaskCh:    make(chan *Task),
		reduceTaskCh: make(chan *Task),
		NReduce:      nReduce,
		Phase:        "MAP",
	}
	go c.dispatchMapTask()
	go c.dispatchReduceTask()
	c.server()
	return &c
}
