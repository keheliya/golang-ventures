package main

import (
	"./govec"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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

// Main server loop

func main() {
	var local_leader_health_me int
	var local_leader_health int
	local_health_me := 0

	current_membership_count := 0
	leader_key := "0"
	member_list := "1"
	new_nodes := "2"
	membership_counter := "3"
	leader_health := "4"

	is_leader := false

	if len(os.Args) != 3 {
		fmt.Println("Usage: ", os.Args[0], "server:port id")
		os.Exit(1)
	}
	service := os.Args[1]

	id_str := os.Args[2]

	logger := govec.Initialize("KVClient"+id_str, "KVClient"+id_str)

	client, err := rpc.Dial("tcp", service)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	var ValReplyArg10 ValReply
	var testSetVec = logger.PrepareSend("test set Vec", nil)
	var getVec = logger.PrepareSend("get Vec", nil)
	var putVec = logger.PrepareSend("put Vec", nil)

	//check if leaders exist
	testSetArgs := TestSetArgs{leader_key, "", id_str, testSetVec}
	err = client.Call("KeyValService.TestSet", &testSetArgs, &ValReplyArg10)
	if err != nil {
		log.Fatal("comm error:", err)
	}
	if ValReplyArg10.Val == id_str {
		fmt.Printf("I'm the leader\n")
		//update member list with my id
		is_leader = true

		var ValReplyArg11 ValReply
		getValArgs := GetArgs{member_list, getVec}
		err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg11)
		if err != nil {
			log.Fatal("comm error:", err)
		}
		resp := ValReplyArg11.Val
		fmt.Printf("member list:%s\n", resp)

		if !strings.Contains(resp, id_str) {
			if resp != "" {
				resp += ("," + id_str)
			} else {
				resp = id_str
			}
			//stringid := fmt.Sprintf("%v", id)
			var ValReplyArg12 ValReply
			PutValArgs := PutArgs{member_list, resp, putVec}
			fmt.Printf("Updating member list with my id: %v\n", PutValArgs.Val)
			err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg12)
			if err != nil {
				// handle error
				fmt.Println(err)
				os.Exit(2)
			}
		}

		//updating mem count
		var ValReplyArg2 ValReply
		PutValArgs := PutArgs{membership_counter, "1", putVec}
		fmt.Printf("Sending leader mem count: %v\n", PutValArgs.Val)
		err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg2)
		if err != nil {
			// handle error
			fmt.Println(err)
			os.Exit(2)
		}

		//updating leader health
		local_leader_health_me = 1
		var ValReplyArg3 ValReply
		PutValArgs = PutArgs{leader_health, fmt.Sprintf("%v", local_leader_health_me), putVec}
		fmt.Printf("Sending my health as leader: %v\n", PutValArgs.Val)
		err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg3)
		if err != nil {
			// handle error
			fmt.Println(err)
			os.Exit(2)
		}

	} else {
		fmt.Printf("Leader already there\n")
		//put myself to new_nodes list
		var ValReplyArg13 ValReply
		getValArgs := GetArgs{new_nodes, getVec}
		err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg13)
		if err != nil {
			log.Fatal("comm error:", err)
		}
		resp := ValReplyArg13.Val

		if !strings.Contains(resp, id_str) {
			if resp != "" {
				resp += ("," + id_str)
			} else {
				resp = id_str
			}
			//stringid := fmt.Sprintf("%v", id)
			var ValReplyArg14 ValReply
			PutValArgs := PutArgs{new_nodes, resp, putVec}
			fmt.Printf("Sending me as new node: %v\n", PutValArgs.Val)
			err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg14)
			if err != nil {
				// handle error
				fmt.Println(err)
				os.Exit(2)
			}
		}

		//get current membership count
		var ValReplyArg15 ValReply
		getValArgs = GetArgs{membership_counter, getVec}
		err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg15)
		if err != nil {
			log.Fatal("comm error:", err)
		}
		resp = ValReplyArg15.Val

		current_membership_count, err := strconv.Atoi(resp)
		if err != nil {
			// handle error
			fmt.Println(err)
			os.Exit(2)
		}
		fmt.Printf("initial membership count:%v\n", current_membership_count)

		//initialize leader health
		getValArgs = GetArgs{leader_health, getVec}
		err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg15)
		if err != nil {
			log.Fatal("comm error:", err)
		}
		resp = ValReplyArg15.Val

		local_leader_health, err := strconv.Atoi(resp)
		if err != nil {
			// handle error
			fmt.Println(err)
			os.Exit(2)
		}
		fmt.Printf("intial leader health:%v\n", local_leader_health)

	}

	ticker := time.NewTicker(time.Millisecond * 1000)
	for _ = range ticker.C {
		if is_leader {
			//if there are new nodes, add them to member list
			var ValReplyArg16 ValReply
			getValArgs := GetArgs{new_nodes, getVec}
			err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg16)
			if err != nil {
				log.Fatal("comm error:", err)
			}
			resp1 := ValReplyArg16.Val
			fmt.Printf("new nodes:%s\n", resp1)

			if resp1 != "" {
				var ValReplyArg17 ValReply
				getValArgs = GetArgs{member_list, getVec}
				err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg17)
				if err != nil {
					log.Fatal("comm error:", err)
				}
				resp2 := ValReplyArg17.Val

				resp2 += ("," + resp1)
				//stringid := fmt.Sprintf("%v", id)
				var ValReplyArg18 ValReply
				PutValArgs := PutArgs{member_list, resp2, putVec}
				fmt.Printf("Sending updated member list (added): %v\n", PutValArgs.Val)
				err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg18)
				if err != nil {
					// handle error
					fmt.Println(err)
					os.Exit(2)
				}

				var ValReplyArg19 ValReply
				PutValArgs = PutArgs{new_nodes, "", putVec}
				fmt.Printf("emtying new_nodes list: %v\n", PutValArgs.Val)
				err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg19)
				if err != nil {
					// handle error
					fmt.Println(err)
					os.Exit(2)
				}

				//get new membership count
				var ValReplyArg20 ValReply
				getValArgs = GetArgs{membership_counter, getVec}
				err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg20)
				if err != nil {
					log.Fatal("comm error:", err)
				}
				resp := ValReplyArg20.Val
				fmt.Printf("209 resp: %v\n", resp)
				new_membership_count, err := strconv.Atoi(resp)
				if err != nil {
					// handle error
					fmt.Println(err)
					os.Exit(2)
				}
				new_membership_count++
				fmt.Printf("new membership count:%v\n", new_membership_count)
				var ValReplyArg21 ValReply
				PutValArgs = PutArgs{membership_counter, fmt.Sprintf("%v", new_membership_count), putVec}
				fmt.Printf("Sending updated mem count: %v\n", PutValArgs.Val)
				err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg21)
				if err != nil {
					// handle error
					fmt.Println(err)
					os.Exit(2)
				}
			}

			//updating leader health
			local_leader_health_me++
			var ValReplyArg2 ValReply
			PutValArgs := PutArgs{leader_health, fmt.Sprintf("%v", local_leader_health_me), putVec}
			fmt.Printf("Updating my health as leader: %v\n", PutValArgs.Val)
			err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg2)
			if err != nil {
				// handle error
				fmt.Println(err)
				os.Exit(2)
			}

		} else {

			//updating my health
			local_health_me++
			var ValReplyArg2 ValReply
			PutValArgs := PutArgs{id_str, fmt.Sprintf("%v", local_health_me), putVec}
			fmt.Printf("Updating my health as slave: %v\n", PutValArgs.Val)
			err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg2)
			if err != nil {
				// handle error
				fmt.Println(err)
				os.Exit(2)
			}

			//get new membership count
			var ValReplyArg22 ValReply
			getValArgs := GetArgs{membership_counter, getVec}
			err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg22)
			if err != nil {
				log.Fatal("comm error:", err)
			}
			resp := ValReplyArg22.Val

			new_membership_count, err := strconv.Atoi(resp)
			if err != nil {
				// handle error
				fmt.Println(err)
				os.Exit(2)
			}

			if new_membership_count != current_membership_count {
				fmt.Printf("new membership count:%v current mem count:%v\n", new_membership_count, current_membership_count)
				var ValReplyArg23 ValReply
				getValArgs = GetArgs{member_list, getVec}
				err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg23)
				if err != nil {
					log.Fatal("comm error:", err)
				}
				resp = ValReplyArg23.Val
				fmt.Printf("received member list:%v\n", resp)
				current_membership_count = new_membership_count

			}
			//get leader id
			getValArgs = GetArgs{leader_key, getVec}
			err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg22)
			if err != nil {
				log.Fatal("comm error:", err)
			}
			current_leader_id := ValReplyArg22.Val

			getValArgs = GetArgs{leader_health, getVec}
			err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg22)
			if err != nil {
				log.Fatal("comm error:", err)
			}
			resp = ValReplyArg22.Val

			real_leader_health, err := strconv.Atoi(resp)
			if err != nil {
				// handle error
				fmt.Println(err)
				os.Exit(2)
			}
			fmt.Printf("leader:%v real health:%v local leader health:%v\n", current_leader_id, real_leader_health, local_leader_health)
			//fmt.Printf("real health:%v local leader health:%v\n", real_leader_health, local_leader_health)
			if local_leader_health < real_leader_health {
				local_leader_health = real_leader_health
			} else {
				fmt.Printf("real health:%v local leader health:%v leader is dead!!!\n", real_leader_health, local_leader_health)
				var ValReplyArg10 ValReply
				var testSetVec = logger.PrepareSend("test set Vec", nil)

				//check if leaders exist
				testSetArgs := TestSetArgs{leader_key, current_leader_id, id_str, testSetVec}
				err = client.Call("KeyValService.TestSet", &testSetArgs, &ValReplyArg10)
				if err != nil {
					log.Fatal("comm error:", err)
				}
				if ValReplyArg10.Val == id_str {
					fmt.Printf("I'm the leader\n")
					is_leader = true

					real_leader_health += 2
					local_leader_health_me = real_leader_health

					var ValReplyArg2 ValReply
					PutValArgs := PutArgs{leader_health, fmt.Sprintf("%v", local_leader_health_me), putVec}
					fmt.Printf("Updating my health as leader: %v\n", PutValArgs.Val)
					err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg2)
					if err != nil {
						// handle error
						fmt.Println(err)
						os.Exit(2)
					}

					var ValReplyArg17 ValReply
					getValArgs = GetArgs{member_list, getVec}
					err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg17)
					if err != nil {
						log.Fatal("comm error:", err)
					}
					resp2 := ValReplyArg17.Val
					fmt.Printf("old member list:%v", resp2)

					resp2 = strings.Replace(resp2, current_leader_id, "dead", -1)
					fmt.Printf("newly created member list:%v\n", resp2)
					var ValReplyArg18 ValReply
					PutValArgs = PutArgs{member_list, resp2, putVec}
					fmt.Printf("Sending updated member list (removed): %v\n", PutValArgs.Val)
					err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg18)
					if err != nil {
						// handle error
						fmt.Println(err)
						os.Exit(2)
					}

					//get new membership count
					var ValReplyArg20 ValReply
					getValArgs = GetArgs{membership_counter, getVec}
					err = client.Call("KeyValService.Get", &getValArgs, &ValReplyArg20)
					if err != nil {
						log.Fatal("comm error:", err)
					}
					resp := ValReplyArg20.Val
					fmt.Printf("209 resp: %v\n", resp)
					new_membership_count, err := strconv.Atoi(resp)
					if err != nil {
						// handle error
						fmt.Println(err)
						os.Exit(2)
					}
					new_membership_count++
					fmt.Printf("new membership count:%v\n", new_membership_count)
					var ValReplyArg21 ValReply
					PutValArgs = PutArgs{membership_counter, fmt.Sprintf("%v", new_membership_count), putVec}
					fmt.Printf("Sending updated mem count: %v\n", PutValArgs.Val)
					err = client.Call("KeyValService.Put", &PutValArgs, &ValReplyArg21)
					if err != nil {
						// handle error
						fmt.Println(err)
						os.Exit(2)
					}

				}

			}

		}

	}

}
