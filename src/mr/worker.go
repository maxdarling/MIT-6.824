package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// get tasks indefinitely (until the coordinator tells us to exit)
	for {
		t := GetTask()

		// empty task: sleep and retry
		if t.Task_id == "" {
			time.Sleep(2000 * time.Millisecond)
			continue
		}

		if t.Task_id[:3] == "map" {
			ret_filenames := mapHelper(&t, mapf)
			CompleteTask(t.Task_id, ret_filenames)
		} else {
			ret_filename := reduceHelper(&t, reducef)
			CompleteTask(t.Task_id, []string{ret_filename})

			partition_num := GetLastToken(t.Task_id, "-")
			cleanupIntermediateFiles(partition_num)
		}
	}
}

func mapHelper(t *Task, mapf func(string, string) []KeyValue) []string {
	// read file + call map to get KVs
	filename := t.Filenames[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	// create R output files
	var writers []*json.Encoder // use json for easy KV reading/writing
	var created_filenames []string

	map_task_num := GetLastToken(t.Task_id, "-")
	f_prefix := "mr-out-" + map_task_num + "-"

	for i := 0; i < t.R; i++ {
		fname := f_prefix + strconv.Itoa(i)
		ofile, _ := os.Create(fname)
		defer ofile.Close()

		enc := json.NewEncoder(ofile)
		writers = append(writers, enc)
		created_filenames = append(created_filenames, fname)
	}

	// write all KV pairs to correct partition file
	for _, kvp := range kva {
		partition := ihash(kvp.Key) % t.R
		err := writers[partition].Encode(&kvp)
		if err != nil {
			panic(err)
		}
	}

	return created_filenames
}

func reduceHelper(t *Task, reducef func(string, []string) string) string {
	partition := GetLastToken(t.Task_id, "-")
	fname := "mr-out-" + partition
	ofile, _ := os.Create(fname)
	var kva []KeyValue

	// collect all KV pairs from files
	for _, filename := range t.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	// merge values for distinct keys, then call reduce for each
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	fmt.Println("[ReduceTask] worker wrote file ", fname)
	return fname
}

// delete all intermediate files used in a reduce task.
func cleanupIntermediateFiles(partition_num string) {
	regex := "\\.\\/mr-out-[0-9]*-" + partition_num
	cmd := exec.Command("find", ".", "-regex", regex, "-delete")
	_, err := cmd.Output()
	if err != nil {
		panic(err)
	}
}

//
// Boilerplate
//

func GetTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply.Task
	} else {
		os.Exit(0) // temp: makes test output cleaner
		panic("worker failed to call master, exiting")
	}
}

func CompleteTask(task_id string, filenames []string) {
	args := CompleteTaskArgs{Task_id: task_id, Filenames: filenames}
	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		os.Exit(0) // temp: makes test output cleaner
		panic("worker failed to call master, exiting")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
