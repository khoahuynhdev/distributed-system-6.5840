package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
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
			ExecuteTask(reply.File, reply.ID, reply.NReduce)
			ws.Status = IDLE
			// doing tasks
		} else {
			fmt.Println("Failed to get task")
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

func GetHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ReadContent(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("cannot open %v", fileName)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

type SyncWriter struct {
	FilePath string
	KVA      []struct {
		Key   string
		Value string
	}
	Ch chan string
	Q  chan string
}

func (w *SyncWriter) Dispatch() {
	c, q := w.Ch, w.Q
	for {
		select {
		case v := <-c:
			// fmt.Println("Adding to dict, value: ", v)
			w.KVA = append(w.KVA, struct {
				Key   string
				Value string
			}{Key: v, Value: "1"})
		case <-q:
			fmt.Printf("file: %s, length: %d\n", w.FilePath, len(w.KVA))
			content := ""
			for _, v := range w.KVA {
				content += fmt.Sprintf("%s, 1\n", v.Key)
			}
			w.Write(content)
			fmt.Printf("Success write file: %s\n", w.FilePath)
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

func (w *SyncWriter) Write(content string) {
	file, err := os.OpenFile(w.FilePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("Get Error opening file %v", err)
		return
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%s%s", content, "\n")
	if err != nil {
		fmt.Printf("failed to write file %v", err)
	}
}

// 3 Map Workers

// (k1, v1) => [](k2,v2)
func MapF(filename, content string, RReducer int, writers []*SyncWriter) error {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	words := strings.FieldsFunc(content, ff)
	var wg sync.WaitGroup
	for _, w := range words {
		wg.Add(1)
		tw := w
		go func() {
			// kva = append(kva, mr.KeyValue{Key: tw, Value: "1"})
			// add the Keva to the right partition by hashing % RReducer
			hashIndex := GetHash(tw) % RReducer
			writers[hashIndex].Ch <- tw
			// fmt.Printf("Append key \"%s\" with hash %d to the table\n", tw, hashIndex)
			wg.Done()
		}()
		// spawn many go routine to write to mr-out-var
	}
	wg.Wait()
	for _, wr := range writers {
		wr.Q <- ""
	}
	return nil
}

func ExecuteTask(fileName string, mapperId, RReducer int) {
	writers := make([]*SyncWriter, RReducer)
	for idx := range writers {
		// file, _ := os.OpenFile(fmt.Sprintf("mr-out-%d", idx), os.O_CREATE|os.O_APPEND, 0644)
		dir, _ := os.Getwd()
		writers[idx] = &SyncWriter{
			FilePath: fmt.Sprintf(dir+"/mr-%d-%d", mapperId, idx),
			Ch:       make(chan string),
			Q:        make(chan string),
		}

		go writers[idx].Dispatch()
	}
	content, err := ReadContent(fileName)
	if err != nil {
		log.Fatalf("cannot open file %v", fileName)
	}
	MapF(fileName, content, RReducer, writers)
}
