package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// RequestType is the type of the rpc request
type RPCType string

const (
	RPCTypeRegisterWorker  = "Coordinator.RegisterWorker"
	RPCTypeAssignMapJob    = "Coordinator.AssignMapJob"
	RPCTypeFinishMapJob    = "Coordinator.FinishMapJob"
	RPCTypeAssignReduceJob = "Coordinator.AssignReduceJob"
	RPCTypeFinishReduceJob = "Coordinator.FinishReduceJob"
	RPCTypeSyncMap         = "Coordinator.SyncMap"
	RPCTypeSyncReduce      = "Coordinator.SyncReduce"
)

type Request struct {
	WorkerId int
	Payload  []byte
}

type Status int

const (
	Success = 200
	Fail    = 300
)

type Response struct {
	Status  Status
	Payload []byte
}

type ResponsePayloadRegisterWorker struct {
	WorkerId int
}

type ResponsePayloadAssignMapJob struct {
	JobId         int
	NReduce       int
	InputFileList []string
}

type RequestPayloadFinishMapJob struct {
	JobId          int
	InputFileList  []string
	OutputFileList []string
}

type ResponsePayloadAssignReduceJob struct {
	JobId         int
	HashKey       int
	NReduce       int
	InputFileList []string
}

type RequestPayloadFinishReduceJob struct {
	JobId      int
	HashKey    int
	OutputFile string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
