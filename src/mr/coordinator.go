package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var id int = 0

func getIncrId() int {
	id++
	return id
}

type JobType string

const (
	JobTypeMap       JobType = "JobTypeMap"
	JobTypeReduce    JobType = "JobTypeReduce"
	JobTypeWait      JobType = "JobTypeWait"
	JobTypeTerminate JobType = "JobTypeTerminate"
)

type Job struct {
	Type             JobType
	JobId            int
	Payload          []byte
	isFinished       bool
	assignedWorkerId int
	assignedTs       time.Time
}

type PayloadJobMap struct {
	Partition     int
	InputFileList []string
}

type PayloadJobReduce struct {
	HashKeyList []int
}

type JobGroup struct {
	Type           JobType
	JobList        []*Job
	FinishedJobCnt int
	InputFileList  []string
	CreatedTs      time.Time
}

type Coordinator struct {
	// system param
	nReduce  int
	files    []string
	jobGroup []*JobGroup

	isDone bool

	// worker info
	mu *sync.Mutex
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.isDone
}

func (c *Coordinator) AskForJob(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, jobGroup := range c.jobGroup {
		if len(jobGroup.JobList) == jobGroup.FinishedJobCnt {
			continue
		}

		for _, job := range jobGroup.JobList {
			if job.isFinished {
				continue
			}
			if job.assignedWorkerId != -1 && time.Since(job.assignedTs).Seconds() < 10. {
				continue
			}

			log.Printf("assigned job-%d to worker-%d", job.JobId, req.WorkerId)
			job.assignedWorkerId = req.WorkerId
			job.assignedTs = time.Now()
			rsp.Payload, _ = json.Marshal(*job)
			rsp.Status = Success
			return nil
		}

		if len(jobGroup.JobList) > jobGroup.FinishedJobCnt {
			log.Printf("%v phase [%d/%d], issuing wait job to worker-%d", jobGroup.Type, jobGroup.FinishedJobCnt, len(jobGroup.JobList), req.WorkerId)
			waitJob := Job{Type: JobTypeWait, JobId: -1}
			rsp.Payload, _ = json.Marshal(waitJob)
			rsp.Status = Success
			return nil
		}
	}

	terminateJob := Job{Type: JobTypeTerminate, JobId: -1}
	rsp.Payload, _ = json.Marshal(terminateJob)
	c.isDone = true
	rsp.Status = Success
	return nil
}

func (c *Coordinator) SubmitJob(req *Request, rsp *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var payload RequestPayloadSubmitJob
	_ = json.Unmarshal(req.Payload, &payload)

	for _, jobGroup := range c.jobGroup {
		if len(jobGroup.JobList) == jobGroup.FinishedJobCnt {
			continue
		}
		for _, job := range jobGroup.JobList {
			if job.JobId == payload.JobId && job.assignedWorkerId == req.WorkerId {
				job.isFinished = true
				jobGroup.FinishedJobCnt++
				rsp.Status = Success
				log.Printf("worker-%d finished job-%d, [%d/%d]", req.WorkerId, payload.JobId, jobGroup.FinishedJobCnt, len(jobGroup.JobList))
				return nil
			}
		}
	}

	rsp.Status = Fail
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(fileList []string, nReduce int) *Coordinator {
	log.Printf("Initiating new coordinator, nReduce %v", nReduce)
	now := time.Now()
	c := Coordinator{
		nReduce: nReduce,
		files:   fileList,
		mu:      &sync.Mutex{},
	}

	mapJobGroup := JobGroup{
		Type:           JobTypeMap,
		FinishedJobCnt: 0,
		InputFileList:  fileList,
		CreatedTs:      now,
	}
	for _, file := range fileList {
		mapJob := Job{
			JobId:            getIncrId(),
			Type:             JobTypeMap,
			isFinished:       false,
			assignedWorkerId: -1,
		}
		mapJob.Payload, _ = json.Marshal(
			PayloadJobMap{InputFileList: []string{file}, Partition: nReduce},
		)
		mapJobGroup.JobList = append(mapJobGroup.JobList, &mapJob)
	}
	c.jobGroup = append(c.jobGroup, &mapJobGroup)

	reduceJobGroup := JobGroup{
		Type:           JobTypeReduce,
		FinishedJobCnt: 0,
		CreatedTs:      now,
	}
	for i := 0; i < nReduce; i++ {
		reduceJob := Job{
			JobId:            getIncrId(),
			Type:             JobTypeReduce,
			isFinished:       false,
			assignedWorkerId: -1,
		}
		reduceJob.Payload, _ = json.Marshal(PayloadJobReduce{HashKeyList: []int{i}})
		reduceJobGroup.JobList = append(reduceJobGroup.JobList, &reduceJob)
	}
	c.jobGroup = append(c.jobGroup, &reduceJobGroup)

	c.server()
	return &c
}
