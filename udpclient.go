package main

import (
	"fmt"
	"net"
)

func main() {
	service := "198.162.52.146:11235"

	//Connecting to server
	serverAddr, err := net.ResolveUDPAddr("udp", service)
	conn, err := net.DialUDP("udp", nil, serverAddr)

	handleError(err, "Could not resolve udp address or connect on: "+service)

	//Close connection later
	defer conn.Close()

	//Writing to connection
	_, err = conn.Write([]byte("hello \n"))
	handleError(err, "error writing data to server: "+service)

	//Receiving response
	data := make([]byte, 1000)
	oob := make([]byte, 3200)
	bytes_read, oobn, flags, _, err := conn.ReadMsgUDP(data, oob)

	debugInfo := fmt.Sprintf("%v bytes and %v out of band bytes received\n flags:\t%v", bytes_read, oobn, flags)
	handleError(err, "Cannot read msg! "+debugInfo)

	//Displaying output
	fmt.Printf("%v\n", string(data))

}

func handleError(err error, msg string) {
	if err != nil {
		fmt.Println("%v", msg)
		panic(err)
	}
}
