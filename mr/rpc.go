package mr

import (
	"github.com/satori/go.uuid"
	"os"
	"strconv"
)

type TaskArgs struct {
	Worker uuid.UUID
}

type TaskReply struct {
	TaskType int
	Number   int
	Filename string
	NMap     int
	NReduce  int
}

type TaskDoneArgs TaskArgs
type Empty struct{}

// Get up a unique-ish UNIX-domain socket name in /var/tmp
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
