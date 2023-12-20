package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// line to run coordinator:
// (rm -f mr-out-* ||:) && go build -race -buildmode=plugin ../mrapps/wc.go && echo "done building" && go run -race mrcoordinator.go pg-*.txt

// todo:
// - breakup files into chunks (use `split -C 64m filename`)
// - check all tests are passing again...

//
// State
//

// The location of a chunk/piece of a file on disk (in the current directory).
type ChunkHandle struct {
	Filename string
	// Offset   int // note: these may change depending on go's file chunk reading interface.
	// Size     int
}

// A "map" or "reduce" task to be delegated to workers by the coordinator.
type Task struct {
	// uniquely identifies tasks.
	// map tasks range from "map-0" to "map-<M-1>" (where M is # of chunks)
	// reduce tasks range from "reduce-0" to "reduce-<R-1>" (where R is # of partitions)
	// both worker and coordinator will parse this string to infer task type and number.
	Task_id string

	// map tasks: specifies 1 chunk to map
	// reduce tasks: specifies a list of entire files (not chunks) to reduce
	Chunk_hanldes []ChunkHandle

	// static fields
	R int // note: i tried making this global but worker's copy was always 0. likely b/c worker is in a diff process.
}

type Coordinator struct {
	// maps used to track queued and in-progress tasks.
	// all keys are Task.Task_ids.
	todo_map_tasks    map[string]Task
	todo_reduce_tasks map[string]Task
	inflight_tasks    map[string]Task

	all_map_tasks_done bool

	// synchronize access
	mu sync.Mutex

	// static fields
	R int
}

//
// Logic
//

// Assign a map or reduce task to a worker.
//
// The task will be reassigned if not completed within a timeout window (10s)
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	defer c.mu.Unlock()
	c.mu.Lock()

	// do all map tasks first, then reduce tasks
	task_queue := c.todo_map_tasks
	if c.all_map_tasks_done {
		task_queue = c.todo_reduce_tasks
	}

	if len(task_queue) == 0 {
		if len(c.inflight_tasks) > 0 {
			fmt.Println("[Coordinator] no tasks to assign at the moment...")
			return nil
		} else {
			return errors.New("mapreduce job is complete. worker, shutdown please")
		}
	}

	// assign a task
	for k, v := range task_queue { // (hack to choose 1 arbitrary map elem)
		reply.Task = v
		delete(task_queue, k)
		break
	}
	c.inflight_tasks[reply.Task.Task_id] = reply.Task

	// fmt.Println("[GetTask] assigned task ", reply.Task.Task_id)

	// launch a timeout-checker that will re-assign the task if not completed within a
	// reasonable time.
	go func(task_id string) {
		time.Sleep(10 * time.Second)

		defer c.mu.Unlock()
		c.mu.Lock()

		task, ok := c.inflight_tasks[task_id]
		if ok {
			fmt.Println("[Timeout Checker] rescheduling timed-out task: ", task_id)
			if task_id[0:3] == "map" {
				c.todo_map_tasks[task_id] = task
			} else {
				c.todo_reduce_tasks[task_id] = task
			}
			delete(c.inflight_tasks, task_id)
		}
	}(reply.Task.Task_id)

	return nil
}

// mark a task completed
//
// returns silently if the task doesn't exist. this can happen if a straggler returns after it's
// task was reassigned and completed by another worker.
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	defer c.mu.Unlock()
	c.mu.Lock()

	_, ok := c.inflight_tasks[args.Task_id]
	if !ok {
		return nil
	}

	if args.Task_id[:3] == "map" {
		// update all corresponding reduce tasks with new intermediate files
		for _, fname := range args.Filenames {
			partition := GetLastToken(fname, "-")
			reduce_task_id := "reduce-" + partition
			reduce_task := c.todo_reduce_tasks[reduce_task_id]
			reduce_task.Chunk_hanldes = append(reduce_task.Chunk_hanldes, ChunkHandle{Filename: fname})
			c.todo_reduce_tasks[reduce_task_id] = reduce_task
		}
	} else {
		// fmt.Printf("[CompleteTask] reduce task completed. final partition file: %v\n", args.Filenames[0])
	}

	// remove task from inflight queue to complete it
	delete(c.inflight_tasks, args.Task_id)

	// one-time step: remember if we just finished all map tasks
	if !c.all_map_tasks_done && len(c.todo_map_tasks) == 0 && len(c.inflight_tasks) == 0 {
		c.all_map_tasks_done = true
		fmt.Printf("---\n---\n[StateChange] all map tasks completed\n---\n---\n")
	}

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	defer c.mu.Unlock()
	c.mu.Lock()

	return len(c.todo_map_tasks) == 0 &&
		len(c.todo_reduce_tasks) == 0 &&
		len(c.inflight_tasks) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// R is the # of reduce tasks to use
func MakeCoordinator(files []string, R int) *Coordinator {
	c := Coordinator{}
	c.todo_map_tasks, c.todo_reduce_tasks = make(map[string]Task), make(map[string]Task)
	c.inflight_tasks = make(map[string]Task)

	//
	// optional: split files
	// note: splitting will cause some tests to fail wrongly (those that depend on the original filenames)
	//
	// var split_files []string
	// for i, file := range files {
	// 	// note: paper does chunks of 64MB, but the books inputs are <1MB.
	// 	ChunkSizeKB := 500
	// 	split_files = append(split_files, splitFile(file, "split_"+strconv.Itoa(i), ChunkSizeKB)...)
	// }
	// files = split_files

	// populate map tasks
	for i, filename := range files {
		task_id := "map-" + strconv.Itoa(i)
		c.todo_map_tasks[task_id] = Task{
			Task_id: task_id,
			Chunk_hanldes: []ChunkHandle{
				{Filename: filename}, // todo: depr. this struct
			},
			R: R,
		}
	}

	// populate reduce tasks
	for i := 0; i < R; i++ {
		task_id := "reduce-" + strconv.Itoa(i)
		c.todo_reduce_tasks[task_id] = Task{
			Task_id: task_id,
			R:       R,
			// other fields populated at assign-time
		}
	}

	fmt.Printf("starting job. M = %v, R = %v\n", len(c.todo_map_tasks), R)
	c.server()
	return &c
}

//
// Boilerplate
//

// split a file into chunks and return said chunk filenames.
// keeps lines intact (required for mapreduce).
// file prefix must be unique.
func splitFile(file string, file_prefix string, chunkSizeKB int) []string {
	size_bytes := strconv.Itoa(chunkSizeKB * 1000)
	cmd := exec.Command(
		"gsplit", "-C", size_bytes, "--numeric-suffixes", "--suffix-length=3", file, file_prefix,
	)
	_, err := cmd.Output()
	if err != nil {
		panic(err)
	}

	// cmd = exec.Command("ls", file_prefix+"*") // bugged for some reason...
	cmd = exec.Command("find", ".", "-name", file_prefix+"*")
	stdout, err2 := cmd.Output()
	if err2 != nil {
		panic(err2)
	}

	splitFn := func(c rune) bool {
		return c == '\n'
	}
	return strings.FieldsFunc(string(stdout), splitFn)
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
