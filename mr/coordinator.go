package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	taskList *taskList
}

type taskList struct {
	tasks []task
	mu    sync.Mutex
}

func (tl *taskList) mappingDone() bool {
	for _, task := range tl.tasks {
		if task.isMap() && !task.isDone() {
			return false
		}
	}
	return true
}

type task struct {
	taskType string
	fileName string
	assigned time.Time
	worker   string
	done     bool
}

func (t *task) isMap() bool                     { return t.taskType == "map" }
func (t *task) isReduce() bool                  { return t.taskType == "reduce" }
func (t *task) isAssigned() bool                { return t.assigned.IsZero() }
func (t *task) isAssignedTo(worker string) bool { return t.worker == worker }
func (t *task) isDone() bool                    { return t.done }
func (t *task) unassign() {
	t.worker = ""
	t.assigned = time.Time{}
}

// Give a task to a querying worker
func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for _, task := range c.taskList.tasks {
		if task.isAssigned() || task.isDone() {
			continue
		}

		if task.isMap() || (task.isReduce() && c.taskList.mappingDone()) {
			reply.Task = task.taskType
			reply.FileName = task.fileName
			task.assigned = time.Now()
			task.worker = args.Worker
			return nil
		}
	}

	// Return indicator that all tasks busy or done
	return nil
}

// A worker signals that the task is done
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for _, task := range c.taskList.tasks {
		if task.isAssignedTo(args.Worker) {
			task.done = true
			return nil
		}
	}

	return nil
}

// Periodically check assigned tasks and unassign task
func (c *Coordinator) checkTasks() {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for _, task := range c.taskList.tasks {
		if task.isAssigned() && !task.isDone() {
			assignTime := task.assigned
			currentTime := time.Now()
			difference := currentTime.Sub(assignTime)
			if difference.Seconds() > 10 {
				task.unassign()
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

	// Make the split
	// Create map tasks

	taskList := taskList{
		tasks: make([]task, 0),
	}

	c := Coordinator{
		&taskList,
	}

	c.server()
	return &c
}
