package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	Map Phase = iota
	Reduce
	Exit
	PhaseMax
)

var nextPhase [][]Phase = make([][]Phase, PhaseMax)

func init() {
	nextPhase[Map] = []Phase{Reduce, Exit, Exit, Exit}
	nextPhase[Reduce] = []Phase{Exit, Exit, Exit, Exit}
	nextPhase[Exit] = []Phase{Exit, Exit, Exit, Exit}
}

func (p *Phase) nextPhase() Phase {
	return nextPhase[*p][*p]
}

type meta struct {
	status    TaskStatus // 任务状态
	startTime time.Time  // 任务开始时间
}

type Master struct {
	// Your definitions here.

	stopCh    chan struct{} // 退出通道
	waitQueue chan *Task    // 异步处理队列, 处理worker的返回结果

	taskQueue chan *Task       // 存储任务
	taskMeta  map[uint64]*meta // 存储任务元数据
	phase     Phase            // 代表当前阶段
	nReduce   int              // 启动几个reduce服务

	lock          sync.Mutex          // 用于保护 intermediates 和 phase
	intermediates map[uint64][]string // 存储intermediate信息, key就是reducerId, value就是文件名
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.phase == Exit
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		stopCh:        make(chan struct{}, 1),
		waitQueue:     make(chan *Task, max(len(files), nReduce)),
		taskQueue:     make(chan *Task, max(len(files), nReduce)),
		taskMeta:      make(map[uint64]*meta),
		phase:         Map,
		nReduce:       nReduce,
		intermediates: make(map[uint64][]string),
	}

	// Your code here.

	// 启动异步处理协程
	go m.schedule()

	// 创建map任务, 论文中是切成16MB-64MB的文件再转换成map task
	// 这里实现则是不用切片, 一个原始文件对应一个map task
	m.initMapTasks(files)

	m.server()
	return &m
}

// 初始化 map 任务
func (m *Master) initMapTasks(files []string) {
	for i, filename := range files {
		task := &Task{
			Id:       uint64(i),
			Type:     MapTask,
			NReducer: m.nReduce,
			Data:     filename,
		}
		m.taskMeta[task.Id] = &meta{
			status: Idle,
		}
		m.taskQueue <- task
	}
}

// 初始化 reduce 任务
func (m *Master) initReduceTasks() {
	for reducerId, files := range m.intermediates {
		task := Task{
			Id:       reducerId,
			Type:     ReduceTask,
			NReducer: m.nReduce,
			Data:     files,
		}
		m.taskMeta[task.Id] = &meta{
			status: Idle,
		}
		m.taskQueue <- &task
	}
}

// 异步处理任务协程
func (m *Master) schedule() {
	// defer log.Println("master schedule exit")
	for {
		select {
		case task := <-m.waitQueue:
			m.handleTask(task)
		case <-m.stopCh:
			return
		default:
			time.Sleep(1 * time.Second)
		}

	}
}

func (m *Master) handleTask(task *Task) {
	// 需要加锁, 避免多线程导致同时进入
	m.lock.Lock()
	defer m.lock.Unlock()

	switch task.Type {
	case MapTask:
		if m.phase != Map {
			return
		}

		// 收集intermediate信息
		paths, ok := task.Data.([]string)
		if !ok {
			log.Fatalln("failed to decode return map task data, task id", task.Id)
			return
		}

		for reduceTaskId, path := range paths {
			m.intermediates[uint64(reduceTaskId)] = append(m.intermediates[uint64(reduceTaskId)], path)
		}
	case ReduceTask:
		if m.phase != Reduce {
			return
		}
	default:
		log.Fatalln("error task type")
	}

	m.taskMeta[task.Id].status = Completed
	if allTaskDone(m.taskMeta) {
		if m.phase == Map {
			// log.Println("master: map tasks done, enter reduce phase")
			m.initReduceTasks()
			m.phase = m.phase.nextPhase()
		} else if m.phase == Reduce {
			// log.Println("master: reduce tasks done, enter exit phase")
			m.stopCh <- struct{}{}
			m.phase = m.phase.nextPhase()
		}
	}
}

// rpc call from worker, get a task
func (m *Master) GetTask(args *ExampleArgs, reply *GetTaskReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.phase == Exit {
		reply.Task = Task{Type: ExitTask}
		return nil
	}

	if len(m.taskQueue) > 0 {
		task := <-m.taskQueue
		m.taskMeta[task.Id].status = InProgress
		m.taskMeta[task.Id].startTime = time.Now()
		reply.Task = *task
	} else {
		reply.Task = Task{Type: WaitTask}
	}

	return nil
}

// rpc call from worker, set a task execute result
func (m *Master) SetTask(args *SetTaskArgs, reply *ExampleReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	task := args.Task
	if (task.Type == MapTask && m.phase != Map) || (task.Type == ReduceTask && m.phase != Reduce) || m.taskMeta[task.Id].status == Completed {
		// 因为worker写在同一个文件磁盘上, 对于重复的结果要丢弃
		return nil
	}

	m.waitQueue <- &task
	return nil
}

// helper functions

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func allTaskDone(meta map[uint64]*meta) bool {
	for _, task := range meta {
		if task.status != Completed {
			return false
		}
	}
	return true
}
