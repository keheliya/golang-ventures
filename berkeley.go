package main

import (
	"flag"
	"fmt"
	"os"
	"bufio"
	"net"
	"time"
	"strings"
	"strconv"
)

import "./govec"

type Connection struct {
	ClientAddr *net.UDPAddr // Address of the client
	ClientConn *net.UDPConn // UDP connection to client
}

func main() {

	//common flags
	timePtr := flag.Int64("time", 0, "Time to be intialized with (milliseconds since the Unix epoch)")
	masterPtr := flag.Bool("m", false, "Behave as a time master")
	slavePtr := flag.Bool("s", false, "Behave as a time slave")
	addressPtr := flag.String("address", "127.0.0.1:8080", "Address to connect to")
	logfilePtr := flag.String("logfile", "test.log", "log file location")

	//server specific flags
	dPtr := flag.Int64("d", 42, "the d threshold used in the fault tolerant average (in milliseconds)")
	slavesfilePtr := flag.String("slavesfile", "slaves.txt", "slaves file location")

	flag.Parse()

	offset := *timePtr * 1000000

	var fixer int64

	start := time.Now().UnixNano()

	if (*masterPtr) {

		fmt.Println("intializing server...")
		fmt.Println("time:", *timePtr)
		fmt.Println("logfile:", *logfilePtr)
		fmt.Println("d:", *dPtr)
		fmt.Println("slavesfile:", *slavesfilePtr)
		fmt.Println("tail:", flag.Args())
		fmt.Println("....")

		Logger := govec.Initialize("Server", *logfilePtr)
		seq_count := 0

		// Mapping from client addresses (as host:port) to connection
		var ClientDict map[string]*Connection = make(map[string]*Connection)

		inFile, _ := os.Open(*slavesfilePtr)
		defer inFile.Close()
		scanner := bufio.NewScanner(inFile)
		scanner.Split(bufio.ScanLines)

		clients := make([]string, 0)

		for scanner.Scan() {
			clients = append(clients, scanner.Text())
		}

		ticker := time.NewTicker(time.Millisecond * 10000)
		go func() {
			for t := range ticker.C {
				now := time.Now().UnixNano()
//				fmt.Println("fixer")
//				fmt.Println(fixer)
				elapsed := now - start + offset + fixer
				time_master := elapsed / 1000000
				seq_count++
				done := make(chan bool)
				var times map[string]int64 = make(map[string]int64)
				var deltas map[string]int64 = make(map[string]int64)
				//var fixes map[string]int64 = make(map[string]int64)
				for _, elem := range clients {

					go func(element string) {

						conn, found := ClientDict[element]
						if !found {
							clientAddr, _ := net.ResolveUDPAddr("udp", element)
							conn = NewConnection(clientAddr)
							if conn == nil {

							}
							ClientDict[element] = conn

						}

						fmt.Sprintf("Tick at", t)
						tickmsg := fmt.Sprintf("%v:time?:\n", seq_count)

						clientConnection := ClientDict[element].ClientConn

						_, err := clientConnection.Write(Logger.PrepareSend("Asking time from "+element, []byte(tickmsg)))

						if err != nil {
							fmt.Println(err)
							//panic(err)
						}
						c1 := make(chan string, 1)
						go func(cc *net.UDPConn) {
							buf := make([]byte, 1600)
							_, _, err := cc.ReadFromUDP(buf)


							if err != nil {
								//fmt.Println(err)
								//fmt.Printf("error! reading!:%v\n", err)
							} else {
								UBuffer := Logger.UnpackReceive("Receive time from "+element, buf)
								request := string(UBuffer)
								c1 <- request
								//fmt.Printf("time>> %v\n", time.Now())
								//fmt.Printf("client %v says:>> %v\n", src, string(buf))
							}
						}(clientConnection)

						select {
						case res := <-c1:
							s := strings.Split(res, "?")
							_, node_time_u := s[0], s[1]

							j, err := strconv.Atoi(string(s[0]))
							if err != nil {
								fmt.Println(err)
							}

							if (j == seq_count) {
								i, err := strconv.ParseInt(string(s[1]), 10, 64)
								if err != nil {
									fmt.Println(err)
								}
								tm := time.Unix(0, i*1000000)
								fmt.Printf("Received:%v:>> as %v\n", tm, node_time_u)

								new_now := time.Now().UnixNano()

								elapsed_here := new_now - start + offset + fixer
								fmt.Printf("Fixed local time:%v:\n", time.Unix(0, elapsed_here))

								times[element] = i
								deltas[element] = i-time_master
								done <- true

							}


							fmt.Sprintf(res)
						case <-time.After(time.Millisecond * 1500):
						done <- true
						}


					}(elem)
				}

				for _, _ = range clients {
					<-done
				}
				//fmt.Println("something happened!")
				//fmt.Println(len(times))
				var count int64 = 1
				var sum int64 = 0
				for _, value := range deltas {
					//fmt.Println(value)
					//fmt.Println(*dPtr)
					n := value
					if n < 0 {
						n = -n
					}
					if (n < *dPtr) {
						count ++
						sum += value

					}
				}
				Logger.LogLocalEvent("Calculating fault tolerant average")
				avg := sum / count
				fmt.Printf("Local Fix:%v:\n",avg)
				fixer += (avg*1000000)
				//				master_fix := avg
				//				fmt.Println(avg)

				for elm, value := range deltas {
					fix := avg - value
					go func(fix int64, elm string) {
						tickmsg := fmt.Sprintf("%v:fix?:%v:", 0, fix)

						clientConnection := ClientDict[elm].ClientConn

						_, err := clientConnection.Write(Logger.PrepareSend("Send fixed time to "+elm, []byte(tickmsg)))
						if err != nil {
							//fmt.Printf("error! sending!:%v\n", err)
						}

					}(fix, elm)
				}

			}
		}()



		var input string
		fmt.Scanln(&input)
		fmt.Println("done")

	} else if (*slavePtr) {

		fmt.Println("intializing client...")
		fmt.Println("address:", *addressPtr)
		fmt.Println("time:", *timePtr)
		fmt.Println("logfile:", *logfilePtr)
		fmt.Println("tail:", flag.Args())
		fmt.Println("....")

		Logger := govec.Initialize("Client-"+*addressPtr, *logfilePtr)



		clientAddr := *addressPtr

		addr, err := net.ResolveUDPAddr("udp", clientAddr)

		if err != nil {
			fmt.Println(err)
		}

		conn, err := net.ListenUDP("udp", addr)



		if err != nil {
			fmt.Println(err)
		}

		in := make(chan struct {
				buf []byte
				src *net.UDPAddr
			}, 16)

		go func(ch chan struct {
				buf []byte
				src *net.UDPAddr
			}, conn *net.UDPConn, cAddr string, start int64, offset int64, fixer *int64) {

			for frame := range ch {
				UBuffer := Logger.UnpackReceive("Receive time request or fix from server", frame.buf)
				request := string(UBuffer)

				//fmt.Printf("connected %v\n", frame.src)
				fmt.Printf("Received:%v\n", request)

				s := strings.Split(request, ":")
				seq, req_type, fix := s[0], s[1], s[2]

				if (strings.Contains(req_type, "time?")) {
					now := time.Now().UnixNano()
//					fmt.Println("fixer")
//					fmt.Println(*fixer)

					elapsed := now - start + offset + *fixer
					resp := fmt.Sprintf("%v?%v?", seq, elapsed/1000000)
					if (strings.Contains(cAddr, "1111")) {
						//	time.Sleep(4000 * time.Millisecond)
					}

					_, _, err := conn.WriteMsgUDP(Logger.PrepareSend("Telling time", []byte(resp)), nil, frame.src)

					if err != nil {
						fmt.Println(err)
					}
				} else if (strings.Contains(req_type, "fix?")) {
					fmt.Sprintf(">>%v\n", fix)
					i, err := strconv.ParseInt(string(fix), 10, 64)
					if err != nil {
						fmt.Println(err)
					}
					log_msg := fmt.Sprintf("Fixing my time by %v ms", i)
					Logger.LogLocalEvent(log_msg)
					*fixer += (i*1000000)

				}




			}

		}(in, conn, clientAddr, start, offset, &fixer)

		for {
			buf := make([]byte, 1600)
			n, src, err := conn.ReadFromUDP(buf)

			if err != nil {
				//fmt.Printf("error! reading!:%v\n", err)
			}

			in <- struct {
					buf []byte
					src *net.UDPAddr
				}{buf[:n], src}
		}

	}




}


// Generate a new connection by opening a UDP connection to the client
func NewConnection(cliAddr *net.UDPAddr) *Connection {
	conn := new(Connection)
	conn.ClientAddr = cliAddr
	fmt.Printf("New connection!%v\n", cliAddr)
	srvudp, err := net.DialUDP("udp", nil, cliAddr)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	conn.ClientConn = srvudp
	return conn
}
