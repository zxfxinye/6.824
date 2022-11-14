package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//CallMapTask()
	//CallTask() 可能是map 也可能是reduce, maptask有剩余时分配maptask, maptask全部执行完毕分配reduce task
	for {
		task := CallTask()
		//fmt.Printf("reply %v\n", task)
		if task == nil {
			return
		}
		if task.Completeflag == 1 {
			//fmt.Printf("mapreduce task is completed\n")
			return
		}
		//fmt.Println(task)
		if task.Tasktype == 0 {
			file, err := os.Open(task.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", task.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", task.Filename)
			}
			file.Close()
			intermediate := [10][]KeyValue{}
			kva := mapf(task.Filename, string(content))
			X := task.TaskId
			for _, kv := range kva {
				Y := ihash(kv.Key) % 10
				intermediate[Y] = append(intermediate[Y], kv)
			}
			for k, v := range intermediate {
				f, err := os.Create("mr-" + strconv.Itoa(X) + "-" + strconv.Itoa(k))
				if err != nil {
					log.Fatal("create intermediate file failed: ", err)
				}
				enc := json.NewEncoder(f)
				for _, kv := range v {
					enc.Encode(&kv)
				}
			}

			//intermediate = append(intermediate, kva...)
		} else {
			n := task.TaskId
			var intermediate []KeyValue
			for i := 0; i < task.Mapcount; i++ {
				file, err := os.Open("mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(n))
				if err != nil {
					log.Fatalf("open file failed %v\n", err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))
			oname := "mr-out-" + strconv.Itoa(n)
			//fmt.Println(oname)
			tmpfile, _ := ioutil.TempFile(os.TempDir(), oname)
			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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
				fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			//fmt.Println(os.TempDir()+oname," ", oname)
			//newpath, _ := filepath.Abs(oname)
			err := os.Rename(tmpfile.Name(), oname)
			if err != nil {
				log.Fatalf("rename failed...%v\n", err)
			}

		}
		CallFinish(task)
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

func CallTask() *Task {
	args := TaskArgs{}
	reply := TaskReply{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply.Task
}

func CallFinish(task *Task) {
	args := TaskArgs{}
	args.Task = task
	call("Coordinator.FinishTask", &args, nil)
}
