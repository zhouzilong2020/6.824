package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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

const tmpDir string = "./tmp"

// dump2File dump intermediate map result to local file.
func dump2File(intermediate []KeyValue, jobId int, nReduce int) []string {
	nReduceKey := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		hash := ihash(kv.Key) % nReduce
		nReduceKey[hash] = append(nReduceKey[hash], kv)
	}

	os.Mkdir(tmpDir, 0777)
	fileList := make([]string, 0)
	wg := sync.WaitGroup{}
	for i := 0; i < nReduce; i++ {
		if len(nReduceKey[i]) == 0 {
			continue
		}
		name := fmt.Sprintf("%s/mr-%v-%v", tmpDir, jobId, i)
		fileList = append(fileList, name)
		wg.Add(1)
		go func(name string, kvList []KeyValue) {
			defer wg.Done()
			fd, err := os.Create(name)
			if err != nil {
				log.Fatalf("fail to create file %v, err %v", name, err)
			}
			defer fd.Close()
			enc := json.NewEncoder(fd)
			for _, kv := range kvList {
				enc.Encode(&kv)
			}
		}(name, nReduceKey[i])
	}

	wg.Wait()
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

func CallAskForJob(workerId int) *Job {
	req := Request{
		WorkerId: workerId,
	}
	rsp := Response{}

	if ok := call(RPCTypeAskForJob, &req, &rsp); ok {
		var job Job
		_ = json.Unmarshal(rsp.Payload, &job)
		return &job
	}

	log.Fatalf("RPC call failed, type: %v", RPCTypeAskForJob)
	return nil
}

func CallSubmitJob(workerId int, jobId int) bool {
	req := Request{
		WorkerId: workerId,
	}
	req.Payload, _ = json.Marshal(RequestPayloadSubmitJob{JobId: jobId})
	rsp := Response{}

	if ok := call(RPCTypeSubmitJob, &req, &rsp); ok {
		return rsp.Status == Success
	}

	log.Fatalf("RPC call failed, type: %v", RPCTypeSubmitJob)
	return false
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	workerId := os.Getpid()
	log.Printf("Worker-%d initiated", workerId)

	for {
		job := CallAskForJob(workerId)
		if job.Type == JobTypeWait {
			time.Sleep(3 * time.Second)
		} else if job.Type == JobTypeMap {
			intermediate := []KeyValue{}
			var payload PayloadJobMap
			_ = json.Unmarshal(job.Payload, &payload)
			for _, file := range payload.InputFileList {
				fd, err := os.Open(file)
				if err != nil {
					log.Fatalf("job-%d cannot open %v", job.JobId, file)
				}
				content, err := ioutil.ReadAll(fd)
				if err != nil {
					log.Fatalf("job-%d cannot read %v", job.JobId, file)
				}
				fd.Close()
				kva := mapf(file, string(content))
				intermediate = append(intermediate, kva...)
			}
			sort.Sort(ByKey(intermediate))
			outputFileList := dump2File(intermediate, job.JobId, payload.Partition)
			if ok := CallSubmitJob(workerId, job.JobId); !ok {
				log.Printf("Fail submit job-%d, deleting output file", job.JobId)
				for _, file := range outputFileList {
					os.Remove(file)
				}
			}
		} else if job.Type == JobTypeReduce {
			var payload PayloadJobReduce
			_ = json.Unmarshal(job.Payload, &payload)
			outputFileList := make([]string, 0)
			for _, hashKey := range payload.HashKeyList {
				dirList, _ := os.ReadDir(tmpDir)
				fileList := make([]string, 0)
				for _, fd := range dirList {
					if fd.IsDir() {
						continue
					}
					key, _ := strconv.ParseInt(strings.Split(fd.Name(), "-")[2], 10, 64)
					if int(key) != hashKey {
						continue
					}
					fileList = append(fileList, fmt.Sprintf("%s/%s", tmpDir, fd.Name()))
				}
				result := make(map[string][]string)
				for _, file := range fileList {
					intermediate := readFromFile(file)
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

				name := fmt.Sprintf("mr-out-%v.txt", hashKey)
				fd, _ := os.Create(name)
				for _, kv := range kvList {
					fmt.Fprintf(fd, "%v %v\n", kv.Key, kv.Value)
				}
				fd.Close()
				outputFileList = append(outputFileList, name)
			}

			if ok := CallSubmitJob(workerId, job.JobId); !ok {
				log.Printf("Fail submit job-%d, deleting output file", job.JobId)
				for _, file := range outputFileList {
					os.Remove(file)
				}
			}
		} else if job.Type == JobTypeTerminate {
			os.RemoveAll(tmpDir)
			return
		}
	}

}
