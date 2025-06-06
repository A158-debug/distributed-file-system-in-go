package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

const bufferSize = 1024

func main() {
	// Listen for incoming TCP connection on PORT:8080
	listenAddr := ":8080"
	listner, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println("Failed to start server", err)
		os.Exit(1)
	}

	defer listner.Close()
	for {
		// Waits for a connection
		conn, err := listner.Accept()
		if err != nil {
			fmt.Println("Error in Accepting", err)
			os.Exit(1)
		}

		// Handle the connection in a new goroutine
		// Multiple connection served concurrently
		go handleRequest(conn)

	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Connection Close :", err)
			return
		}

		fmt.Println("Received :", message)
	}

}
