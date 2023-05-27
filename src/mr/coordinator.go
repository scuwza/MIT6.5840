package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	State       int
	MapChan     chan *Task
	MapTasks    []*Task
	ReduceChan  chan *Task
	ReduceTasks []*Task
	RemainTasks int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Heartbeat(request *HearBeatRequest, response *HearBeatResponse) error {
	mutex.Lock()
	defer mutex.Unlock()
	*response = HearBeatResponse{
		Id: request.Id,
	}
	switch c.State {
	case Map:
		if len(c.MapChan) > 0 {
			response.Task = <-c.MapChan
			response.JobType = MapJob
		} else {
			if !request.waitStatus {
				response.JobType = WaitJob
			} else {
				i := 0
				for i < len(c.MapTasks) {
					if c.MapTasks[i].Finished == false && time.Since(c.MapTasks[i].Start) > time.Second*15 {
						response.Task = c.MapTasks[i]
						response.JobType = MapJob
						c.MapTasks[i].Start = time.Now()
						break
					}
					i++
				}
			}
		}
	case Reduce:
		if len(c.ReduceChan) > 0 {
			response.Task = <-c.ReduceChan
			response.JobType = ReduceJob
		} else {
			if !request.waitStatus {
				response.JobType = WaitJob
			} else {
				i := 0
				for i < len(c.ReduceChan) {
					if c.ReduceTasks[i].Finished == false && time.Since(c.ReduceTasks[i].Start) > time.Second*15 {
						response.Task = c.ReduceTasks[i]
						response.JobType = ReduceJob
						c.ReduceTasks[i].Start = time.Now()
						break
					}
					i++
				}
			}
		}
	case Complete:
		response.JobType = CompleteJob
	}

	return nil
}

func (c *Coordinator) CompleteTask(request *CompleteTaskResquest, response *CompleteTaskResponse) error {
	task := request.Task
	switch task.TaskType {
	case MapJob:
		mutex.Lock()
		c.MapTasks[request.Task.TaskId].Finished = true
		c.RemainTasks--
		mutex.Unlock()
	case ReduceJob:
		mutex.Lock()
		c.ReduceTasks[request.Task.TaskId].Finished = true
		c.RemainTasks--
		mutex.Unlock()
	}
	mutex.Lock()
	if c.Check(task.NReduce) {
		c.State = Complete
	}
	mutex.Unlock()

	return nil
}

func (c *Coordinator) Check(nReduce int) bool {
	if c.RemainTasks == 0 {
		if len(c.ReduceTasks) == 0 {
			c.CreateReduce(nReduce)
			return false
		}
		return true
	}
	return false
}

func (c *Coordinator) CreateReduce(nReduce int) {
	for i := 0; i < nReduce; i++ {
		task := &Task{
			TaskType: ReduceJob,
			Filename: "mr-",
			NReduce:  len(c.MapTasks),
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		c.ReduceTasks = append(c.ReduceTasks, task)
		c.ReduceChan <- task

	}
	c.State = Reduce
	c.RemainTasks = nReduce
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.State == Complete

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:       Map,
		MapChan:     make(chan *Task, len(files)),
		ReduceChan:  make(chan *Task, nReduce),
		MapTasks:    make([]*Task, len(files)),
		ReduceTasks: make([]*Task, 0),
		RemainTasks: len(files),
	}
	i := 0
	for _, file := range files {
		task := Task{
			TaskType: MapJob,
			Filename: file,
			NReduce:  nReduce,
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		c.MapChan <- &task
		//c.MapTasks = append(c.MapTasks,&task)
		c.MapTasks[i] = &task
		i++
	}
	c.server()
	return &c
}
