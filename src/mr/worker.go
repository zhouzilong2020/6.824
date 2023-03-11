package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
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

// dump2File dump intermediate map result to local file.
func dump2File(intermediate []KeyValue, jobId int, nReduce int) []string {
	nReduceKey := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		hash := ihash(kv.Key) % nReduce
		nReduceKey[hash] = append(nReduceKey[hash], kv)
	}

	fileList := make([]string, 0)
	wg := sync.WaitGroup{}
	for i := 0; i < nReduce; i++ {
		if len(nReduceKey[i]) == 0 {
			continue
		}
		name := fmt.Sprintf("mr-%v-%v", jobId, i)
		fileList = append(fileList, name)
		wg.Add(1)
		go func(name string, kvList []KeyValue) {
			fd, _ := os.Create(name)
			defer fd.Close()
			enc := json.NewEncoder(fd)
			for _, kv := range kvList {
				enc.Encode(&kv)
			}
		}(name, nReduceKey[i])
	}

	wg.Done()
	return fileList
}

// readFromFile reconstruct intermediate map result from a local file.
func readFromFile(name string) []KeyValue {
	fd, _ := os.Open(name)
	defer fd.Close()
	dec := json.NewDecoder(fd)
	kva := make([]KeyValue, 0)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// we use the Unix timestamp to id the worker, this may collide, but ignore for now.
	workerId := os.Getpid()
	// log.Printf("Worker-%d initiated", workerId)

	// ask for a map task  (a split of file).
	{
		rsp := CallAssignMapJob(workerId)
		for len(rsp.InputFileList) > 0 {
			intermediate := []KeyValue{}
			for _, filename := range rsp.InputFileList {
				// log.Printf("Parsing %v", filename)
				file, err := os.Open(filename)
				if err != nil {
					// log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					// log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
				// log.Printf("Successfully finish parsing %v", filename)
			}
			sort.Sort(ByKey(intermediate))
			// dump to local file.
			oFileNameList := dump2File(intermediate, rsp.JobId, rsp.NReduce)

			if ok := CallFinishMapJob(workerId, rsp.InputFileList, oFileNameList); !ok {
				// TODO : retry here
				// log.Fatal("Fail to CallFinishJob, exit.")
			}

			// try to ask for more job.
			rsp = CallAssignMapJob(workerId)
		}
	}

	// wait for all map task to finish.
	CallSync(workerId, RPCTypeSyncMap)

	// try to ask for more job.
	{
		rsp := CallAssignReduceJob(workerId)
		for rsp.HashKey != -1 {
			result := make(map[string][]string)
			for _, filename := range rsp.InputFileList {
				intermediate := readFromFile(filename)
				i := 0
				for i < len(intermediate) {
					j := i + 1
					key := intermediate[i].Key
					for j < len(intermediate) && intermediate[j].Key == key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					result[key] = append(result[key], values...)
					i = j
				}
			}

			kvList := make([]KeyValue, 0)
			for k, v := range result {
				kvList = append(kvList, KeyValue{k, reducef(k, v)})
			}
			sort.Sort(ByKey(kvList))

			name := fmt.Sprintf("mr-out-%v.txt", rsp.HashKey)
			fd, _ := os.Create(name)
			for _, kv := range kvList {
				fmt.Fprintf(fd, "%v %v\n", kv.Key, kv.Value)
			}
			fd.Close()

			if ok := CallFinishReduceJob(workerId, rsp.HashKey, name); !ok {
				// TODO : retry here?
				// log.Fatal("Fail to CallFinishJob, exit.")
			}
			rsp = CallAssignReduceJob(workerId)
		}
	}

	// wait for all map task to finish.
	CallSync(workerId, RPCTypeSyncReduce)
}

// CallSync calls Sync defined in coordinator.
func CallSync(workerId int, syncType RPCType) {
	req := Request{
		WorkerId: workerId,
	}
	rsp := Response{}

	call(string(syncType), &req, &rsp)
}

// CallFinishJob calls FinishJob defined in coordinator.
func CallFinishMapJob(workerId int, inputFileList, outputFileList []string) bool {
	req := Request{
		WorkerId: workerId,
	}
	req.Payload, _ = json.Marshal(
		RequestPayloadFinishMapJob{
			InputFileList:  inputFileList,
			OutputFileList: outputFileList,
		},
	)
	rsp := Response{}

	if ok := call(RPCTypeFinishMapJob, &req, &rsp); ok {
		// log.Printf("Response from %v, response: %v", RPCTypeFinishMapJob, rsp)
		return rsp.Status == Success
	}

	// log.Printf("[%v] failed", RPCTypeFinishMapJob)
	return false
}

// CallFinishJob calls FinishJob defined in coordinator.
func CallFinishReduceJob(workerId int, hashKey int, outputFile string) bool {
	req := Request{
		WorkerId: workerId,
	}
	req.Payload, _ = json.Marshal(
		RequestPayloadFinishReduceJob{
			HashKey:    hashKey,
			OutputFile: outputFile,
		},
	)
	rsp := Response{}

	if ok := call(RPCTypeFinishReduceJob, &req, &rsp); ok {
		// log.Printf("Response from %v, response: %v", RPCTypeFinishReduceJob, rsp)
		return rsp.Status == Success
	}

	// log.Printf("%v failed", RPCTypeFinishReduceJob)
	return false
}

// CallAssignMapJob calls AssignMapJob defined in coordinator.
func CallAssignMapJob(workerId int) *ResponsePayloadAssignMapJob {
	req := Request{
		WorkerId: workerId,
	}
	rsp := Response{}

	if ok := call(RPCTypeAssignMapJob, &req, &rsp); ok {
		var payload ResponsePayloadAssignMapJob
		if err := json.Unmarshal([]byte(rsp.Payload), &payload); err != nil {
			// log.Fatalf("Error in Unmarshal payload, err: %v", err)
		}

		// log.Printf("Response from %v, response %v", RPCTypeAssignMapJob, payload)
		return &payload
	}

	// log.Printf("%v failed", RPCTypeAssignMapJob)
	return nil
}

// CallAssignReduceJob calls AssignReduceJob defined in coordinator.
func CallAssignReduceJob(workerId int) *ResponsePayloadAssignReduceJob {
	req := Request{
		WorkerId: workerId,
	}
	rsp := Response{}

	if ok := call(RPCTypeAssignReduceJob, &req, &rsp); ok {
		var payload ResponsePayloadAssignReduceJob
		if err := json.Unmarshal([]byte(rsp.Payload), &payload); err != nil {
			// log.Printf("Error in Unmarshal payload, err: %v", err)
		}

		// log.Printf("Response from %v, response %v", RPCTypeAssignReduceJob, payload.HashKey)
		return &payload
	}

	// log.Printf("%v failed", RPCTypeAssignReduceJob)
	return nil
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
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
