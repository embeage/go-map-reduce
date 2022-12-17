package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"github.com/satori/go.uuid"
)

type Coordinator struct {
	taskList     *taskList
	taskDeadline time.Duration
	nReduceTasks int
}

type taskList struct {
	tasks []task
	mu    sync.Mutex
}

func (tl *taskList) addTask(taskType string, fileName string) {
	tl.tasks = append(tl.tasks, task{taskType: taskType, fileName: fileName})
}
func (tl *taskList) assign(i int, w uuid.UUID, t time.Time) {
	tl.tasks[i].worker = w
	tl.tasks[i].assigned = t
}
func (tl *taskList) unassign(i int) {
	tl.tasks[i].worker = uuid.UUID{}
	tl.tasks[i].assigned = time.Time{}
}
func (tl *taskList) setDone(i int) { tl.tasks[i].done = true }
func (tl *taskList) mappingDone() bool {
	for _, task := range tl.tasks {
		if task.isMap() && !task.isDone() {
			return false
		}
	}
	return true
}
func (tl *taskList) addMapTasks(files []string) {

	for _, file := range files {
		tl.addTask("map", file)
	}
}

type task struct {
	taskType string
	fileName string
	assigned time.Time
	worker   uuid.UUID
	done     bool
}

func (t *task) isMap() bool                        { return t.taskType == "map" }
func (t *task) isReduce() bool                     { return t.taskType == "reduce" }
func (t *task) isAssigned() bool                   { return !t.assigned.IsZero() }
func (t *task) isAssignedTo(worker uuid.UUID) bool { return uuid.Equal(t.worker, worker) }
func (t *task) isDone() bool                       { return t.done }

// Give a task to a querying worker
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for i, task := range c.taskList.tasks {
		if task.isAssigned() || task.isDone() {
			continue
		}

		if task.isMap() || (task.isReduce() && c.taskList.mappingDone()) {
			reply.Task = task.taskType
			reply.FileName = task.fileName
			c.taskList.assign(i, args.WorkerId, time.Now())
			return nil
		}
	}

	// Reply being empty indicates that there was no tasks
	return nil
}

// A worker signals that the task is done
// When TaskDone and it is a map, add a new Reduce task to the list
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

// Periodically check assigned tasks and unassign task
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
		tasks: make([]task, 0),
	}
	taskList.addMapTasks(files)
	
	c := Coordinator{
		taskList: &taskList,
		taskDeadline: 10 * time.Second,
		nReduceTasks: nReduce,
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
