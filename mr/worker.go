package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
	"sort"
	"github.com/satori/go.uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// start a worker
	id := uuid.NewV4()

	for {
		ok := CallTask(id, mapf, reducef)
		if !ok {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func CallTask(id uuid.UUID, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	args := TaskArgs{
		WorkerId: id,
	}

	reply := TaskReply{}

	ok := call("Coordinator.Task", &args, &reply)
	if ok {
		if reply.Task == "map" {
			handleMap(mapf, reply)
		} else if reply.Task == "reduce" {
			handleReduce(reducef, reply)
		}
		CallTaskDone(id)
	} else {
		return false
	}
	return true
}

func CallTaskDone(id uuid.UUID) {

	args := TaskDoneArgs{
		WorkerId: id,
	}

	reply := TaskDoneReply{}

	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
	} else {
	}
}

func handleMap(mapf func(string, string) []KeyValue, task TaskReply) {
	// check if s3 path or local
	intermediate := []KeyValue{}

	if isS3File(task.Filename) {
	} else {
		f, err := os.Open(task.Filename)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer f.Close()
		content, err := ioutil.ReadAll(f)
		if err != nil {
			fmt.Printf("cannot read %v\n", task.Filename)
			return
		}
		kva := mapf(task.Filename, string(content))
		intermediate = append(intermediate, kva...)

		tmpFiles := make([]*os.File, task.NReduce)
		for i := 0; i < len(tmpFiles); i++ {
			tmpFile, err := ioutil.TempFile("", "")
			if err != nil {
				fmt.Println(err)
				return
			}
			tmpFiles[i] = tmpFile
		}

		for _, kv := range intermediate {
			// Make sure same words go in same reduce file, ihash
			tmpFile := tmpFiles[ihash(kv.Key)%task.NReduce]
			enc := json.NewEncoder(tmpFile)
			err = enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
				return
			}
		}

		for i, tmpFile := range tmpFiles {
			tmpFile.Close()
			// Atomically rename file
			filename := fmt.Sprintf("mr-%d-%d", task.Number, i)
			err = os.Rename(tmpFile.Name(), filename)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}

func handleReduce(reducef func(string, []string) string, task TaskReply) {
	if isS3File(task.Filename) {

	} else {

		intermediate := []KeyValue{}

		// Open all files with the correct reduce number
		for i := 0; i < task.NMap; i++ {
			f, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.Number))
			if err != nil {
				fmt.Println(err)
				return
			}
			defer f.Close()
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
	
			}
		}

		sort.Sort(ByKey(intermediate))

		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer tmpFile.Close()

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)
	
			i = j
		}
		outFilename := fmt.Sprintf("mr-out-%d", task.Number)
		err = os.Rename(tmpFile.Name(), outFilename)
		if err != nil {
			fmt.Println(err)
			return
		}
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
		return false
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
