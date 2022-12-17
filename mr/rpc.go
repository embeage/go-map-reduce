package mr

//
// RPC definitions.
//

import "os"
import "strconv"
import "github.com/satori/go.uuid"

type TaskArgs struct {
	WorkerId uuid.UUID
}

type TaskReply struct {
	Task     string
	FileName string
}

type TaskDoneArgs struct {
	WorkerId uuid.UUID
}

type TaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
