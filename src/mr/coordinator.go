package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	tasklist []*Task //task list
	//rlist []*Task //reducetask list
	fcount       int // 已完成的task数
	mapcount     int
	mu           sync.Mutex
	completeflag int
	cond         *sync.Cond
}
type Task struct {
	Filename     string
	TaskId       int
	Status       int8
	Tasktype     int8 //0-maptask; 1-reducetask
	Mapcount     int
	Completeflag int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishTask(args *TaskArgs, reply *TaskReply) error {
	task := args.Task
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tasklist[task.TaskId].Status = 2
	c.mu.Unlock()

	c.mu.Lock()
	c.fcount++
	if task.Tasktype == 0 && c.fcount == len(c.tasklist) { //map任务全部完成后清空tasklist 一次性添加n个reduce任务
		c.cond.Broadcast() //完成所有的maptask时唤醒wait的所有worker进程准备reduce
		c.tasklist = c.tasklist[:0]
		for i := 0; i < 10; i++ {
			reduceTask := Task{TaskId: i, Tasktype: 1, Mapcount: c.mapcount}
			c.tasklist = append(c.tasklist, &reduceTask)
		}
	}
	if c.fcount == c.mapcount+10 { //完成所有任务
		fmt.Println("jobs completed")
		c.completeflag = 1
	}
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.completeflag == 1 { //通知worker任务以及结束
			reply.Task = &Task{Completeflag: 1}
			break
		}
		for _, mt := range c.tasklist {
			if mt.Status == 0 {
				reply.Task = mt
				break
			}
		}
		if reply.Task != nil {
			reply.Task.Status = 1
			go c.checkTaskStatus(reply.Task)
			break
		}
		c.cond.Wait()
	}
	return nil
}

func (c *Coordinator) checkTaskStatus(task *Task) {
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.Status != 2 {
		c.cond.Broadcast()
		task.Status = 0
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.completeflag == 1 {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	var i int
	for _, file := range files {
		mtask := &Task{Filename: file, TaskId: i, Tasktype: 0}
		i++
		c.tasklist = append(c.tasklist, mtask)
	}
	c.cond = sync.NewCond(&c.mu)
	c.mapcount = len(files)
	// Your code here.

	c.server()
	return &c
}
