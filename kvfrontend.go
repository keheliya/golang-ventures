package main

import (
	"./govec"
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"

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

// args in report(args)
type ReportArgs struct {
	Id     string // ID to report backend node's health
	VStamp []byte // vstamp(nil)
}

type HealthService int
type KeyValService int

// The PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

var pq PriorityQueue

var logger *govec.GoLog

var nodeHealth map[string]int

//this keeps the mapping between keys and the respective kvnode
var keyNodeMap map[string]string

// Main server loop.
func main() {
	// parse args
	//ip_port : address of the key-value service to be used by clients
	//ip_port_kv : address to be used by kv nodes
	//logfile : filename to which GoVector log messages will be written
	usage := fmt.Sprintf("Usage: %s ip:port ip:port(for KV) log_file\n", os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	ip_port := os.Args[1]

	ip_port_kv := os.Args[2]

	log_file := os.Args[3]

	fmt.Printf("IP:PORT: %s, IP:PORT(for KV):%s Log_File:%s\n", ip_port, ip_port_kv, log_file)

	logger = govec.Initialize("KV_FrontEnd", log_file)

	nodeHealth = make(map[string]int)
	keyNodeMap = make(map[string]string)
	pq = make(PriorityQueue, 0)
	heap.Init(&pq)

	// setup health service

	logger.LogLocalEvent("Setting up Health Service for Backend nodes")
	healthservice := new(HealthService)
	rpc.Register(healthservice)
	l, e := net.Listen("tcp", ip_port_kv)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	//setup kv service
	logger.LogLocalEvent("Setting up KV service for clients")
	kvservice := new(KeyValService)
	rpc.Register(kvservice)
	k, e := net.Listen("tcp", ip_port)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go func() {
		ticker := time.NewTicker(time.Millisecond * 1000)
		for _ = range ticker.C {
			go checkHealth()
		}
	}()

	go func() {
		for {
			conn, _ := l.Accept()
			go rpc.ServeConn(conn)
		}
	}()

	for {
		conn, _ := k.Accept()
		go rpc.ServeConn(conn)
	}

}

//Handling failed KV nodes discovered during health check
func checkHealth() error {
	for key, value := range nodeHealth {

		logger.LogLocalEvent("Periodic health check for back end nodes")
		nodeHealth[key] = value - 1
		if nodeHealth[key] == 0 {
			logger.LogLocalEvent(key + " node is dead!")
			delete(nodeHealth, key)
			for kvkey, node := range keyNodeMap {
				if node == key {
					keyNodeMap[kvkey] = unavail
				}
			}
			for i, _ := range pq {
				if pq[i].value == key {
					// This is for avoiding the node when new insertions happen
					pq[i].priority = 10000
				}
			}
		}

	}
	if len(nodeHealth) != 0 {
		fmt.Print("Active Nodes:")
		for key, _ := range nodeHealth {
			fmt.Print(key, ",")
		}
		fmt.Print("\n")

		for i, _ := range pq {
			fmt.Print(pq[i].value, ",")
			fmt.Print(pq[i].priority, ",")
		}
		fmt.Print("\n")
	}

	return nil
}

//Handling failed KV nodes discovered during communication
func manageFailure(address string) error {
	logger.LogLocalEvent(address + " node is dead! Handling the failure")
	delete(nodeHealth, address)
	for kvkey, node := range keyNodeMap {
		if node == address {
			keyNodeMap[kvkey] = unavail
		}
	}
	for i, _ := range pq {
		if pq[i].value == address {
			// This is for avoiding this node when new insertions happen
			pq[i].priority = 10000
		}
	}

	return nil
}

// REPORT
func (hs *HealthService) Report(args *ReportArgs, reply *ValReply) error {
	logger.UnpackReceive("health-update from:"+args.Id, args.VStamp)
	if nodeHealth[args.Id] == 0 {
		logger.LogLocalEvent(args.Id + " joined!!")
		fmt.Printf("%s Node reporting health for the first time\n", args.Id)
		item := &Item{
			value:    args.Id,
			priority: 0,
		}
		heap.Push(&pq, item)
	}

	nodeHealth[args.Id] = 5
	reply.Val = "success"
	reply.VStamp = logger.PrepareSend("health-report-ack to:"+args.Id, nil)
	return nil
}

func lookupNode(key string) (string, bool) {
	// lookup key in all nodes
	val := keyNodeMap[key]
	isNew := false
	if val == "" {
		logger.LogLocalEvent("Get a new node to insert from Priority Queue")
		fmt.Println("Not exisiting key:", key, " Preparing a new node for insertion.")
		item := heap.Pop(&pq).(*Item)
		val = item.value
		isNew = true
		fmt.Println(val, " selected.")
		heap.Push(&pq, item)
	}
	return val, isNew
}

// GET
func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {
	fmt.Println("Got get:", args.Key)

	address := keyNodeMap[args.Key]

	if address == "" {
		fmt.Println("Not found!!")
		reply.Val = ""
	} else if address == unavail {
		fmt.Println("Key unavailable!!")
		reply.Val = unavail
	} else {
		fmt.Println("Should be at:", address)
		backendReply := new(ValReply)
		getArgs := GetArgs{args.Key, logger.PrepareSend("get for:"+args.Key, nil)}
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			log.Println("dialing error:", err)
			manageFailure(address)
			return err
		}
		err = client.Call("KeyValService.Get", getArgs, &backendReply)
		if err != nil {
			log.Println("listen error:", err)
			manageFailure(address)
			return err
		}
		reply.Val = backendReply.Val
		logger.UnpackReceive("got response for get req:"+args.Key, backendReply.VStamp)

	}

	reply.VStamp = logger.PrepareSend("sending response for get req", nil)

	return nil

}

// PUT
func (kvs *KeyValService) Put(args *PutArgs, reply *ValReply) error {

	//todo handle node unavailable case
	fmt.Println("Got put", args.Key)
	address, isNew := lookupNode(args.Key)
	fmt.Println("Trying to dial:", address)
	backendReply := new(ValReply)
	if (address == "") || (address == unavail) {
		log.Println("No nodes to serve or key has become unavailable")
		reply.Val = unavail
		reply.VStamp = logger.PrepareSend("put-re:", nil)
		return nil
	}
	putArgs := PutArgs{args.Key, args.Val, logger.PrepareSend("put for:"+args.Key, nil)}
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Println("[Error] dial error:", err)
		manageFailure(address)
		reply.Val = ""
		return nil
	}
	err = client.Call("KeyValService.Put", putArgs, &backendReply)
	if err != nil {
		log.Println("client call error:", err)
		manageFailure(address)
		reply.Val = ""
		return nil
	}
	fmt.Println("Received response:", backendReply.Val)
	logger.UnpackReceive("got response for put:"+args.Key, backendReply.VStamp)
	if isNew {
		logger.LogLocalEvent("Change Priority of Node with new insert")
		//Modifying the index
		keyNodeMap[args.Key] = address
		for i, _ := range pq {
			if pq[i].value == address {
				pq[i].priority = pq[i].priority + 1
			}
		}
	}

	reply.Val = backendReply.Val
	reply.VStamp = logger.PrepareSend("put-re:", nil)

	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *TestSetArgs, reply *ValReply) error {

	fmt.Println("Got testset", args.Key)
	address, isNew := lookupNode(args.Key)
	fmt.Println("Trying to dial:", address)
	backendReply := new(ValReply)
	testsetArgs := TestSetArgs{args.Key, args.TestVal, args.NewVal, logger.PrepareSend("test set for:"+args.Key, nil)}
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Println("dial error:", err)
		manageFailure(address)
		reply.Val = ""
		return nil
	}
	err = client.Call("KeyValService.TestSet", testsetArgs, &backendReply)
	if err != nil {
		log.Println("client call error:", err)
		manageFailure(address)
		reply.Val = ""
		return nil
	}
	fmt.Println("Received response:", backendReply.Val)
	logger.UnpackReceive("got response for test set:"+args.Key, backendReply.VStamp)

	if isNew {
		keyNodeMap[args.Key] = address
		for i, _ := range pq {
			if pq[i].value == address {
				pq[i].priority = pq[i].priority + 1
			}
		}
	}

	reply.Val = backendReply.Val
	reply.VStamp = logger.PrepareSend("testset-re:", nil)
	return nil
}

// An Item is something we manage in a priority queue.
type Item struct {
	value    string // The value of the item; arbitrary.
	priority int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value string, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
