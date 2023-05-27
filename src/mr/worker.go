package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	getRequestId := makeIncrementer()
	waitStatus := false
	for {
		response := doHeartbeat(getRequestId(), waitStatus)
		waitStatus = false
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapF, response)
		case ReduceJob:
			doReduceTask(reduceF, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
			waitStatus = true
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
}

func doReduceTask(reducef func(string, []string) string, response HearBeatResponse) {
	task := response.Task
	i := 0
	intermediate := []KeyValue{}
	for i < task.NReduce {
		filepath := task.Filename + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskId)
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
		i++
	}
	sort.Sort(ByKey(intermediate))
	i = 0
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
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
	call("Coordinator.CompleteTask", CompleteTaskResquest{
		Id:      response.Id,
		Task:    response.Task,
		JobType: ReduceJob,
	},
		&CompleteTaskResponse{})
}

func doMapTask(mapf func(string, string) []KeyValue, response HearBeatResponse) {
	task := response.Task
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	intermediate := []KeyValue{}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	intermediateMap := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		intermediateMap[ihash(kv.Key)%task.NReduce] = append(intermediateMap[ihash(kv.Key)%task.NReduce], kv)
	}

	i := 0
	for i < task.NReduce {
		oname := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediateMap[i] {
			enc.Encode(&kv)
		}
		i++
		ofile.Close()
	}
	call("Coordinator.CompleteTask", CompleteTaskResquest{
		Id:      response.Id,
		Task:    response.Task,
		JobType: MapJob,
	}, &CompleteTaskResponse{})
}

func makeIncrementer() func() int {
	var x int
	return func() int {
		x++
		return x
	}
}

func doHeartbeat(id int, waitStatus bool) HearBeatResponse {
	args := HearBeatRequest{
		Id:         id,
		waitStatus: waitStatus,
	}
	reply := HearBeatResponse{}
	call("Coordinator.Heartbeat", &args, &reply)
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
