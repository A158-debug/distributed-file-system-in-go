package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go [PORT] [STORAGE_DIR]")
		return
	}
	port := os.Args[1]
	storageDir := os.Args[2]

	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		fmt.Println("Error creating storage directory:", err)
		return
	}

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	fmt.Printf("Node listening on port %s, storing chunks in %s\n", port, storageDir)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn, storageDir)
	}
}

func handleConnection(conn net.Conn, storageDir string) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Read header: CHUNK|chunkID|size\n
	// Reads bytes from the connection until it encounters a newline character (\n)
	header, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error reading header:", err)
		return
	}
	header = strings.TrimSpace(header)
	parts := strings.Split(header, "|")
	if len(parts) != 3 || parts[0] != "CHUNK" {
		fmt.Println("Invalid header:", header)
		return
	}
	chunkID := parts[1]
	size, err := strconv.Atoi(parts[2])
	if err != nil {
		fmt.Println("Invalid chunk size:", parts[2])
		return
	}
	fmt.Printf("Receiving chunk: %s (%d bytes)\n", chunkID, size)

	// Create file to write chunk
	chunkPath := filepath.Join(storageDir, chunkID)
	file, err := os.Create(chunkPath)
	if err != nil {
		fmt.Println("Error creating chunk file:", err)
		return
	}
	defer file.Close()

	// Copy chunk data from connection to file
	written, err := io.CopyN(file, reader, int64(size))
	if err != nil {
		fmt.Println("Error receiving chunk data:", err)
		return
	}
	fmt.Printf("Chunk %s stored (%d bytes)\n", chunkID, written)
}
