package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

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

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		// worker从master获取任务
		task := getTask()

		// 拿到task之后, 根据task的state, map task交给mapper, reduce task交给reducer
		// 额外加两个state，让 worker 等待或者直接退出
		switch task.Type {
		case MapTask:
			mapper(task, mapf)
		case ReduceTask:
			reducer(task, reducef)
		case WaitTask:
			time.Sleep(1 * time.Second)
			continue
		case ExitTask:
			return
		}

		report(task)
	}
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("failed to dialing", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func getTask() *Task {
	reply := GetTaskReply{}
	call("Master.GetTask", &ExampleArgs{}, &reply)
	return &reply.Task
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// 从文件名读取content
	filename, ok := task.Data.(string)
	if !ok {
		log.Fatal("failed to get map task, task id", task.Id)
	}

	// 将content交给mapf, 缓存结果
	intermediates := mapf(filename, string(readFromLocalFile(filename)))

	// 缓存后的结果会写到本地磁盘, 并切成R份
	// 切分方式是根据key做hash
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		// 对key进行hash, 分散到不同的slot中
		// 因为hash函数相同, 因此同样的单词在不同的worker中都会hash到同一个slot中
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}

	mapOutput := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		dir, _ := os.Getwd()

		tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		if err != nil {
			log.Fatal("failed to create temp file", err)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range buffer[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatal("failed to write kv pair", err)
			}
		}

		tempFile.Close()
		oname := fmt.Sprintf("mr-%d-%d", task.Id, i)
		os.Rename(tempFile.Name(), oname)

		// log.Println("map output", oname, os.Getpid())

		mapOutput = append(mapOutput, filepath.Join(dir, oname))
	}
	// 保存intermediates的R个文件的位置, 需要发送给master
	task.Data = mapOutput
}

func reducer(task *Task, reducef func(string, []string) string) {
	filenames, ok := task.Data.([]string)
	if !ok {
		log.Fatalln("failed to decode reduce task data, task id =", task.Id)
	}

	intermediate := make([]KeyValue, 0)
	// 从 filename 中读取数据
	for _, filename := range filenames {
		dec := json.NewDecoder(strings.NewReader(string(readFromLocalFile(filename))))
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	//根据kv排序
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("failed to create temp file", err)
	}

	// 直接照搬 sequential 即可
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(tempFile.Name(), oname)

	// log.Println("reduce output", oname, os.Getpid())

	task.Data = oname
}

func report(task *Task) {
	// 通过RPC, 把task信息发给master
	call("Master.SetTask", SetTaskArgs{Task: *task}, nil)
}

func readFromLocalFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("failed to open %v, %v", filename, err)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("failed to read %v, %v", filename, err)
	}
	return content
}
