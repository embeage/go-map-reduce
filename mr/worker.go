package mr

import (
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// For sorting keyValue by key.
type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	id      uuid.UUID
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	bucket  *s3Bucket
}

func (w *worker) handleMap(task TaskReply) {
	var f *os.File
	var err error
	if UseS3 {
		f, err = w.bucket.downloadFile(task.Filename)
	} else {
		f, err = os.Open(task.Filename)
	}
	if err != nil {
		ErrorLogger.Fatal(err)
	}
	defer f.Close()

	content, err := ioutil.ReadAll(f)
	if err != nil {
		ErrorLogger.Fatal(err)
	}

	// Get the key value pairs from the map function
	kva := w.mapf(task.Filename, string(content))

	// Open NReduce tmpfiles
	tmpFiles := make([]*os.File, task.NReduce)
	for i := 0; i < len(tmpFiles); i++ {
		tmpFile, err := ioutil.TempFile("", "mrintermediate.")
		if err != nil {
			ErrorLogger.Fatal(err)
		}
		tmpFiles[i] = tmpFile
	}

	// Write the kv data into the tmpfiles
	for _, kv := range kva {
		// the tmpfile to write into decided by hash of key
		tmpFile := tmpFiles[ihash(kv.Key)%task.NReduce]
		enc := json.NewEncoder(tmpFile)
		err = enc.Encode(&kv)
		if err != nil {
			ErrorLogger.Fatal(err)
		}
	}

	// Rename the tmpfiles to the completed intermediate files
	for i, tmpFile := range tmpFiles {
		tmpFile.Close()
		filename := fmt.Sprintf("mr-%d-%d", task.Number, i)
		err = os.Rename(tmpFile.Name(), filename)
		if UseS3 {
			err = w.bucket.uploadFile(filename)
		}
		if err != nil {
			ErrorLogger.Fatal(err)
		}
	}
}

func (w *worker) handleReduce(task TaskReply) {
	intermediate := []KeyValue{}

	// Open all intermediate files for the task
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.Number)
		var f *os.File
		var err error
		if UseS3 {
			f, err = w.bucket.downloadFile(filename)
		} else {
			f, err = os.Open(filename)
		}
		if err != nil {
			ErrorLogger.Fatal(err)
		}
		defer f.Close()

		// Read the contents of each file into intermediate
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// Sort the intermediate data to align the same keys
	sort.Sort(byKey(intermediate))

	tmpFile, err := ioutil.TempFile("", "mrout.")
	if err != nil {
		ErrorLogger.Fatal(err)
	}
	defer tmpFile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// Group values of the same key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// Call the reduce function on the (now) unique key
		output := w.reducef(intermediate[i].Key, values)

		// Write the final result for the key value pair into the tmpfile
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// Rename the tmpfile to the completed out file
	filename := fmt.Sprintf("mr-out-%d", task.Number)
	err = os.Rename(tmpFile.Name(), filename)
	if UseS3 {
		err = w.bucket.uploadFile(filename)
	}
	if err != nil {
		ErrorLogger.Fatal(err)
	}
}

func (w *worker) callTask() (TaskReply, error) {
	args := TaskArgs{Worker: w.id}
	reply := TaskReply{}
	err := call("Coordinator.Task", &args, &reply)
	return reply, err
}

func (w *worker) callTaskDone() error {
	args := TaskDoneArgs{Worker: w.id}
	err := call("Coordinator.TaskDone", &args, &Empty{})
	return err
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	InitLogger("worker")

	w := worker{
		id:      uuid.NewV4(),
		mapf:    mapf,
		reducef: reducef,
	}
	
	InfoLogger.Printf("Started a worker with UUID %s.\n", w.id)

	if UseS3 {
		w.bucket = newS3Bucket(Region, Bucket)
	}

	// Periodically ask for a task until done
	for {
		time.Sleep(1 * time.Second)

		reply, err := w.callTask()
		if err != nil {
			InfoLogger.Printf("Calling task returned error: %s. Assuming done.\n", err)
			break
		}

		switch reply.TaskType {
		case Map:
			InfoLogger.Printf("Received map task %d, handling...\n", reply.Number)
			w.handleMap(reply)
		case Reduce:
			InfoLogger.Printf("Received reduce task %d, handling...\n", reply.Number)
			w.handleReduce(reply)
		default:
			InfoLogger.Println("No tasks available.")
			continue
		}

		InfoLogger.Println("Done.")

		err = w.callTaskDone()
		if err != nil {
			WarningLogger.Printf("Calling task done returned error: %s. "+
				"This should not happen unless the worker timed out.\n", err)
			break
		}
	}
}

// Send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args interface{}, reply interface{}) error {

	var client *rpc.Client
	var err error

	if TCP {
		client, err = rpc.DialHTTP("tcp", CoordinatorIP+":"+CoordinatorPort)
	} else {
		sockname := coordinatorSock()
		client, err = rpc.DialHTTP("unix", sockname)
	}

	if err != nil {
		return err
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
	return err
}
