package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		panic(err)
	}

	defer conn.Close()

	chunkData := "Hello this is Abhishek Kumar, I am Software Developer"
	chunkID := " chunk_001"

	message := fmt.Sprintf("STORE|%s\n%s", chunkID, chunkData)
	fmt.Println(message)
	conn.Write([]byte(message))
}
