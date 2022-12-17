package mr

import (
	"github.com/satori/go.uuid"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type task struct {
	taskType  string
	number    int
	filename  string
	assigned  time.Time
	worker    uuid.UUID
	done      bool
}

func (t *task) isMap() bool                   { return t.taskType == "map" }
func (t *task) isReduce() bool                { return t.taskType == "reduce" }
func (t *task) isAssigned() bool              { return !t.assigned.IsZero() }
func (t *task) isAssignedTo(w uuid.UUID) bool { return uuid.Equal(t.worker, w) }
func (t *task) isDone() bool                  { return t.done }

type taskList struct {
	tasks        []task
	mu           sync.Mutex
}

func (tl *taskList) addMapTask(i int, filename string) {
	tl.tasks = append(tl.tasks, task{taskType: "map", filename: filename, number: i})
}

func (tl *taskList) addReduceTask(i int) {
	tl.tasks = append(tl.tasks, task{taskType: "reduce", number: i})
}

func (tl *taskList) assign(i int, w uuid.UUID, t time.Time) {
	tl.tasks[i].worker = w
	tl.tasks[i].assigned = t
}

func (tl *taskList) unassign(i int) {
	tl.tasks[i].worker = uuid.UUID{}
	tl.tasks[i].assigned = time.Time{}
}

func (tl *taskList) setDone(i int) {
	tl.tasks[i].done = true
}

func (tl *taskList) mappingDone() bool {
	for _, task := range tl.tasks {
		if task.isMap() && !task.isDone() {
			return false
		}
	}
	return true
}

func (tl *taskList) addMapTasks(files []string) {
	for i, file := range files {
		tl.addMapTask(i, file)
	}
}

func (tl *taskList) addReduceTasks(nReduce int) {
	for i := 0; i < nReduce; i++ {
		tl.addReduceTask(i)
	}
}

type Coordinator struct {
	taskList     *taskList
	taskDeadline time.Duration
	nMap         int
	nReduce      int
}

// Coordinator replies with an available task
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for i, task := range c.taskList.tasks {
		if task.isAssigned() || task.isDone() {
			continue
		}

		if task.isMap() || (task.isReduce() && c.taskList.mappingDone()) {
			reply.Task = task.taskType
			reply.Filename = task.filename
			reply.Number = task.number
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			c.taskList.assign(i, args.WorkerId, time.Now())
			return nil
		}
	}

	// Empty reply indicates no available task
	return nil
}

// Coordinator marks task as done
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for i, task := range c.taskList.tasks {
		if task.isAssignedTo(args.WorkerId) {
			c.taskList.setDone(i)
			c.taskList.unassign(i)
			return nil
		}
	}

	return nil
}

// Coordinator checks for tasks that haven't been completed in time and unassigns them
func (c *Coordinator) checkTasks() {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for i, task := range c.taskList.tasks {
		if task.isAssigned() && !task.isDone() {
			assignTime := task.assigned
			currentTime := time.Now()
			difference := currentTime.Sub(assignTime)
			if difference > c.taskDeadline {
				c.taskList.unassign(i)
			}
		}
	}
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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	taskList := taskList{
		tasks:    []task{},
	}
	taskList.addMapTasks(files)
	taskList.addReduceTasks(nReduce)

	c := Coordinator{
		taskList:     &taskList,
		taskDeadline: 10 * time.Second,
		nMap:		  len(files),
		nReduce:      nReduce,
	}

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			c.checkTasks()
		}
	}()

	c.server()
	return &c
}
