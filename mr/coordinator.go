package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"github.com/satori/go.uuid"
)

type task struct {
	taskType int
	number   int
	filename string
	assigned time.Time
	worker   uuid.UUID
	done     bool
}

func (t task) isMap() bool                   { return t.taskType == Map }
func (t task) isReduce() bool                { return t.taskType == Reduce }
func (t task) isAssigned() bool              { return !t.assigned.IsZero() }
func (t task) isAssignedTo(w uuid.UUID) bool { return uuid.Equal(t.worker, w) }
func (t task) isDone() bool                  { return t.done }
func (t task) String() string {
	switch t.taskType {
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	default:
		return ""
	}
}

type taskList struct {
	tasks []task
	mu    sync.Mutex
}

func (tl *taskList) addMapTask(n int, filename string) {
	tl.tasks = append(tl.tasks, task{taskType: Map, number: n, filename: filename})
}

func (tl *taskList) addReduceTask(n int) {
	tl.tasks = append(tl.tasks, task{taskType: Reduce, number: n})
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
	done         chan bool
	bucket       *s3Bucket
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
			reply.TaskType = task.taskType
			reply.Filename = task.filename
			reply.Number = task.number
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			c.taskList.assign(i, args.Worker, time.Now())
			InfoLogger.Printf("%s task %d assigned to worker %s.\n",
				task, task.number, args.Worker)
			return nil
		}
	}

	// Empty reply indicates no available task
	return nil
}

// Coordinator marks task as done
func (c *Coordinator) TaskDone(args *TaskDoneArgs, _ *Empty) error {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for i, task := range c.taskList.tasks {
		if task.isAssignedTo(args.Worker) {
			c.taskList.setDone(i)
			c.taskList.unassign(i)
			InfoLogger.Printf("%s task %d completed by worker %s.\n",
				task, task.number, args.Worker)
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
			if time.Since(task.assigned) > c.taskDeadline {
				c.taskList.unassign(i)
				InfoLogger.Printf("%s task %d not completed in time by worker "+
					"%s, unassigned.\n", task, task.number, task.worker)
			}
		}
	}
}

// Start an RPC listener
func (c *Coordinator) server() error {
	rpc.Register(c)
	rpc.HandleHTTP()

	var ln net.Listener
	var err error

	if TCP {
		ln, err = net.Listen("tcp", ":"+CoordinatorPort)
	} else {
		sockname := coordinatorSock()
		os.Remove(sockname)
		ln, err = net.Listen("unix", sockname)
	}

	if err != nil {
		return err
	}
	go http.Serve(ln, nil)
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.taskList.mu.Lock()
	defer c.taskList.mu.Unlock()

	for _, task := range c.taskList.tasks {
		if !task.isDone() {
			return false
		}
	}

	InfoLogger.Println("Job finished.")

	if UseS3 {
		// Download the finished files.
	}

	c.done <- true
	return true
}

// Create a new coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	InitLogger("coordinator")

	taskList := taskList{
		tasks: []task{},
	}
	taskList.addMapTasks(files)
	taskList.addReduceTasks(nReduce)
	done := make(chan bool)

	c := Coordinator{
		taskList:     &taskList,
		taskDeadline: 10 * time.Second,
		nMap:         len(files),
		nReduce:      nReduce,
		done:         done,
	}

	if UseS3 {
		c.bucket = newS3Bucket(Region, Bucket)
		c.bucket.removeFiles()
		for _, filename := range files {
			c.bucket.uploadFile(filename)
		}
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				c.checkTasks()
			}
		}
	}()

	err := c.server()
	if err != nil {
		ErrorLogger.Fatal(err)
	} else {
		InfoLogger.Printf("Started a coordinator with %d map tasks "+
			"and %d reduce tasks.\n", len(files), nReduce)
	}

	return &c
}
