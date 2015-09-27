package main

import (
	"./govec"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

// args in get(args)
type GetArgs struct {
	Key    string // key to look up
	VStamp []byte // vstamp(nil)
}

// args in put(args)
type PutArgs struct {
	Key    string // key to associate value with
	Val    string // value
	VStamp []byte // vstamp(nil)
}

// args in testset(args)
type TestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
	VStamp  []byte // vstamp(nil)
}

// Reply from service for all three API calls above.
type ValReply struct {
	Val    string // value; depends on the call
	VStamp []byte // vstamp(nil)
}

// Value in the key-val store.
type MapVal struct {
	value  string       // the underlying value representation
	logger *govec.GoLog // GoVector instance for the *key* that this value is mapped to
}

// Map implementing the key-value store.
var kvmap map[string]*MapVal

var logger *govec.GoLog

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"

type KeyValService int

// args in report(args)
type ReportArgs struct {
	Id     string // ID to report backend node's health
	VStamp []byte // vstamp(nil)
}

// Main server loop.
func main() {
	// parse args
	usage := fmt.Sprintf("Usage: %s ip:port logfile\n", os.Args[0])
	if len(os.Args) != 3 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	//address of the front-end node
	ip_port := os.Args[1]

	log_file := os.Args[2]

	fmt.Printf("IP:PORT: %s, Log_File:%s\n", ip_port, log_file)

	// setup key-value store and register service
	kvmap = make(map[string]*MapVal)
	kvservice := new(KeyValService)
	rpc.Register(kvservice)
	l, e := net.Listen("tcp", ":0")
	defer l.Close()

	own_address := l.Addr().String()

	logger = govec.Initialize(own_address, log_file)

	fmt.Printf("KeyValue service backend started at %s\n", own_address)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for _ = range ticker.C {
			go updateHealth(ip_port, own_address)
		}
	}()

	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}

func updateHealth(ip_port string, own_id string) error {
	var ValReplyArg ValReply
	var updateHealthVec = logger.PrepareSend("update health", nil)

	client, err := rpc.Dial("tcp", ip_port)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Synchronous call

	reportValArgs := ReportArgs{own_id, updateHealthVec}
	err = client.Call("HealthService.Report", &reportValArgs, &ValReplyArg)
	if err != nil {
		log.Fatal("comm error:", err)
	}
	resp := ValReplyArg.Val
	fmt.Printf("Health Update: %s\n", resp)
	logger.UnpackReceive("health-report ack from front end", ValReplyArg.VStamp)

	return nil
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Lookup a key, and if it's used for the first time, then initialize its value.
func lookupKey(key string) *MapVal {
	// lookup key in store
	val := kvmap[key]
	if val == nil {
		// key used for the first time: create and initialize a MapVal instance to associate with a key
		val = &MapVal{
			value:  "",
			logger: govec.Initialize("key-"+key, "key-"+key),
		}
		kvmap[key] = val
	}
	return val
}

// GET
func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {
	fmt.Printf("Received Get for: %s\n", args.Key)
	val := lookupKey(args.Key)
	logger.UnpackReceive("get(k:"+args.Key+")", args.VStamp)

	reply.Val = val.value // execute the get
	reply.VStamp = logger.PrepareSend("get-re:"+val.value, nil)
	return nil
}

// PUT
func (kvs *KeyValService) Put(args *PutArgs, reply *ValReply) error {
	fmt.Printf("Received Put for: %s\n", args.Key)
	val := lookupKey(args.Key)
	logger.UnpackReceive("put(k:"+args.Key+",v:"+args.Val+")", args.VStamp)

	val.value = args.Val // execute the put
	reply.Val = ""
	reply.VStamp = logger.PrepareSend("put-re", nil)
	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *TestSetArgs, reply *ValReply) error {
	val := lookupKey(args.Key)
	logger.UnpackReceive("testset(k:"+args.Key+",tv:"+args.TestVal+",nv:"+args.NewVal+")", args.VStamp)

	// execute the testset
	if val.value == args.TestVal {
		val.value = args.NewVal
	}

	reply.Val = val.value
	reply.VStamp = logger.PrepareSend("testset-re:"+val.value, nil)
	return nil
}
