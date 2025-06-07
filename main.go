package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

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
		header, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Connection Close :", err)
			return
		}

		header = strings.TrimSpace(header)
		parts := strings.Split(header, "|")
		if len(parts) != 3 || parts[0] != "STORE" {
			fmt.Println("Invalid header:", header)
			return
		}
		fileName := parts[1]
		fileSize, err := strconv.Atoi(parts[2])
		if err != nil {
			fmt.Println("Invalid filesize:", parts[2])
			return
		}

		fmt.Printf("Receiving file: %s (%d bytes)\n", fileName, fileSize)
		
		// Create file to write
		file, err := os.Create(fileName)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer file.Close()

		// Copy file data from connection to file
		written, err := io.CopyN(file, reader, int64(fileSize))
		if err != nil {
			fmt.Println("Error receiving file data:", err)
			return
		}
		fmt.Printf("File %s received (%d bytes)\n", fileName, written)
	}

}
