package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var id int = 0

func getIncrId() int {
	id++
	return id
}

type Coordinator struct {
	// system param
	nReduce int
	files   []string

	mapAssignment  map[string]int       // file -> workerId
	mapIsFinished  map[string]bool      // file -> isFinished
	mapAssignedTs  map[string]time.Time // file -> Assigned Ts
	mapResultFiles []string
	mapIsDone      bool

	reduceAssignment  map[int]int       // hashKey -> workerId
	reduceIsFinished  map[int]bool      // hashKey -> isFinished
	reduceAssignedTs  map[int]time.Time // hashKey -> Assigned Ts
	reduceResultFiles []string
	reduceIsDone      bool

	isDone bool

	// worker info
	mu         *sync.Mutex
	mapCond    *sync.Cond
	reduceCond *sync.Cond
}

// AssignMapJob assigns a Map job to a worker.
func (c *Coordinator) RegisterWorker(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return nil
}

// AssignMapJob assigns a Map job to a worker.
func (c *Coordinator) AssignMapJob(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, fileName := range c.files {
		// the file has been processed
		if val, ok := c.mapIsFinished[fileName]; ok && val {
			continue
		}
		// the job has been assigned for 10s, still no reading, try to assigned to another
		if createdTs, ok := c.mapAssignedTs[fileName]; ok && time.Since(createdTs).Seconds() < 12. {
			continue
		} else if ok {
			log.Printf("reassign %v to worker-%v from worker-%d", fileName, req.WorkerId, c.mapAssignment[fileName])
		}

		jid := getIncrId()
		log.Printf("Assigned %v as job-%d to worker-%d", fileName, jid, req.WorkerId)
		c.mapAssignment[fileName] = req.WorkerId
		c.mapAssignedTs[fileName] = time.Now()
		rsp.Payload, _ = json.Marshal(
			ResponsePayloadAssignMapJob{
				JobId:         jid,
				NReduce:       c.nReduce,
				InputFileList: []string{fileName},
			},
		)
		rsp.Status = Success
		return nil
	}

	if c.mapIsDone {
		rsp.Payload, _ = json.Marshal(
			ResponsePayloadAssignMapJob{
				JobId:   -1,
				NReduce: 0,
			},
		)
		return nil
	}

	// no more task to assign. Workers should continue to ask for job in case of some job crash.
	rsp.Status = Success
	rsp.Payload, _ = json.Marshal(
		ResponsePayloadAssignMapJob{
			JobId:   0,
			NReduce: 0,
		},
	)
	return nil
}

// FinishJob mark a job as finished.
func (c *Coordinator) FinishMapJob(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var payload RequestPayloadFinishMapJob
	json.Unmarshal([]byte(req.Payload), &payload)

	for _, file := range payload.InputFileList {
		c.mapIsFinished[file] = true
		delete(c.mapAssignment, file)
		delete(c.mapAssignedTs, file)
		log.Printf("%s finished", file)
	}
	c.mapResultFiles = append(c.mapResultFiles, payload.OutputFileList...)

	if len(c.mapIsFinished) == len(c.files) {
		finishCnt := 0
		for _, file := range c.files {
			if val, ok := c.mapIsFinished[file]; ok && val {
				finishCnt++
			}
		}
		if finishCnt == len(c.files) {
			c.mapIsDone = true
			c.mapCond.Broadcast()
		}
	}

	log.Printf("job-%d finished.", payload.JobId)
	rsp.Status = Success
	return nil
}

// Sync hold the rpc until all the map task is finished.
func (c *Coordinator) SyncMap(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("worker-%d called sync", req.WorkerId)

	for !c.mapIsDone {
		c.mapCond.Wait()
	}

	rsp.Status = Success
	log.Printf("worker-%d return from syncMap, len(mapResultFiles)=%d", req.WorkerId, len(c.mapResultFiles))
	return nil
}

// AssignReduceJob assigns a Reduce job to a worker.
// It returns a hash key for a worker to work on.
func (c *Coordinator) AssignReduceJob(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.nReduce; i++ {
		// the file has been processed
		if val, ok := c.reduceIsFinished[i]; ok && val {
			continue
		}
		// the job has been assigned for 12s, still no reading, try to assigned to another
		if createdTs, ok := c.reduceAssignedTs[i]; ok && time.Since(createdTs).Seconds() < 12. {
			continue
		} else if ok {
			log.Printf("reassign %v to worker-%v from worker-%d", i, req.WorkerId, c.reduceAssignedTs[i])
		}

		jid := getIncrId()
		log.Printf("Assigned reduce-%v as job-%d to worker-%d", i, jid, req.WorkerId)
		c.reduceAssignment[i] = req.WorkerId
		inputFileList := make([]string, 0)
		for _, file := range c.mapResultFiles {
			if strings.Split(file, "-")[2] != strconv.Itoa(i) {
				continue
			}
			inputFileList = append(inputFileList, file)
		}
		rsp.Payload, _ = json.Marshal(
			ResponsePayloadAssignReduceJob{
				JobId:         jid,
				HashKey:       i,
				NReduce:       c.nReduce,
				InputFileList: inputFileList,
			},
		)

		rsp.Status = Success
		return nil
	}

	if c.reduceIsDone {
		rsp.Payload, _ = json.Marshal(
			ResponsePayloadAssignReduceJob{
				JobId:   -1,
				HashKey: -1,
				NReduce: 0,
			},
		)
		return nil
	}

	// no more task to assign
	rsp.Status = Success
	rsp.Payload, _ = json.Marshal(
		ResponsePayloadAssignReduceJob{
			JobId:   0,
			HashKey: -1,
			NReduce: 0,
		},
	)
	return nil
}

// FinishJob mark a job as finished.
func (c *Coordinator) FinishReduceJob(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var payload RequestPayloadFinishReduceJob
	json.Unmarshal([]byte(req.Payload), &payload)

	c.reduceResultFiles = append(c.reduceResultFiles, payload.OutputFile)
	c.reduceIsFinished[payload.HashKey] = true
	delete(c.reduceAssignment, payload.HashKey)
	delete(c.reduceAssignedTs, payload.HashKey)

	if c.nReduce == len(c.reduceResultFiles) {
		c.reduceIsDone = true
		c.reduceCond.Broadcast()
	}

	log.Printf("job-%d finished.", payload.JobId)
	rsp.Status = Success
	return nil
}

// Sync hold the rpc until all the reduce task is finished.
func (c *Coordinator) SyncReduce(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.reduceIsDone {
		c.reduceCond.Wait()
	}

	rsp.Status = Success
	log.Printf("worker-%d return from syncReduce", req.WorkerId)
	c.isDone = true
	return nil
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
	log.Printf("Server started, listening on port %v", sockname)
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("Initiating new coordinator, nReduce %v, files %v", nReduce, files)
	c := Coordinator{
		nReduce: nReduce,
		files:   files,
		mu:      &sync.Mutex{},

		mapIsFinished:    make(map[string]bool),
		mapAssignment:    make(map[string]int),
		mapAssignedTs:    make(map[string]time.Time),
		reduceIsFinished: make(map[int]bool),
		reduceAssignment: make(map[int]int),
		reduceAssignedTs: make(map[int]time.Time),
	}
	c.mapCond = sync.NewCond(c.mu)
	c.reduceCond = sync.NewCond(c.mu)

	c.server()
	return &c
}
