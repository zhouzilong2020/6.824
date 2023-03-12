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
	RPCTypeAskForJob = "Coordinator.AskForJob"
	RPCTypeSubmitJob = "Coordinator.SubmitJob"
)

type Request struct {
	WorkerId int
	Payload  []byte
}

type RequestPayloadSubmitJob struct {
	JobId          int
	OutputFileList []string
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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
