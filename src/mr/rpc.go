package mr

import (
	"os"
	"strconv"
)

// GetTask RPC
//
// special cases:
//   - an empty task ID means there are no currently assignable tasks, and that
//     the worker should check back soon.
//   - the coordinator will send an error when all jobs are done, signaling the worker may exit
type GetTaskArgs struct{}

type GetTaskReply struct {
	Task Task
}

// CompleteTask RPC
type CompleteTaskArgs struct {
	Task_id string

	// the filenames of the files created by the worker
	// map: for task ID "map-K", output files take form "mr-out-K-i" where i is the partition # (in [0, R))
	// reduce: for task ID "reduce-J", output files take form "mr-out-J"
	Filenames []string
}

type CompleteTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
