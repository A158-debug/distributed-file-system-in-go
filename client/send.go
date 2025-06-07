package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {

	file, err := os.Open("sample.txt")
	if err != nil {
		fmt.Println("Error in opening a file", err)
		return
	}
	defer file.Close()

	// Get file Infor for size
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file Infor:", err)
		return
	}

	fileSize := fileInfo.Size()
	fileName := fileInfo.Name()

	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	// chunkData := "Hello this is Abhishek Kumar, I am Software Developer"
	// chunkID := " chunk_001"
	// message := fmt.Sprintf("STORE|%s\n%s", chunkID, chunkData)
	// conn.Write([]byte(message))

	// Send header : STORE|filename|filesize\n
	header := fmt.Sprintf("STORE|%s|%d\n", fileName, fileSize)
	_, err = conn.Write([]byte(header))
	if err != nil {
		fmt.Println("Error in sending header:", err)
		return
	}

	// Send File Data
	writer := bufio.NewWriter(conn)
	sent, err := io.Copy(writer, file)
	if err != nil {
		fmt.Println("Error sending file data:", err)
		return
	}
	writer.Flush()
	fmt.Printf("File %s sent (%d bytes)\n", fileName, sent)
}
