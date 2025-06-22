package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

type ChunkInfo struct {
	ChunkID   string   `json:"chunk_id"`
	Locations []string `json:"locations"`
}

func main() {

	filename := "sample.txt"
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error in opening a file", err)
		return
	}
	defer file.Close()

	//Split file into chunks
	chunks := [][]byte{}
	for {
		buf := make([]byte, 1024)
		n, err := file.Read(buf)
		if n > 0 {
			chunks = append(chunks, buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error in reading the file:", err)
			return
		}
	}
	fmt.Printf("File split into %d chunks\n", len(chunks))

	numChunks := len(chunks)
	replicationFactor := 3 // Number of replicas for each chunk

	// Ask master for chunk palcement
	placements := getChunkPlacements(filename, numChunks, replicationFactor)
	if len(placements) != numChunks {
		fmt.Println("Mismatch in chunk placements")
		return
	}
	// Send each chunks to assigned nodes
	/*
		chunks = [
		  [ Chunk 1 data ],
		  [ Chunk 2 data ],
		  [ Chunk 3 data ],
		  ...
		 ]
	*/
	for i, chunk := range chunks {
		chunkID := placements[i].ChunkID
		locations := placements[i].Locations

		for _, nodeAddr := range locations {
			err := sendChunkToNode(nodeAddr, chunkID, chunk)
			if err != nil {
				fmt.Printf("Error sending chunk %s to %s: %v\n", chunkID, nodeAddr, err)
			} else {
				fmt.Printf("Chunk %s sent to %s\n", chunkID, nodeAddr)
			}
		}
	}
	registerChunksWithMaster(filename, placements)
	fmt.Println("All chunks sent and registered with master.")
}

func getChunkPlacements(filename string, numChunks int, replicationFactor int) []ChunkInfo {
	conn, err := net.Dial("tcp", ":9100")
	if err != nil {
		fmt.Println("Error connecting in master :", err)
		return nil
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	req := map[string]interface{}{
		"type":              "get_chunk_placement",
		"filename":          filename,
		"num_chunks":        numChunks,
		"replication_factor": replicationFactor,
	}

	encoder.Encode(req)

	var placement []ChunkInfo
	if err := decoder.Decode(&placement); err != nil {
		fmt.Println("Error in decoding the placement response:", err)
		return nil
	}

	fmt.Println("Received chunk placements from master:", placement)
	/**
	Example of placement response:
	placement :
	   [
		  { ChunkID: "sample.txt_chunk_000", Locations: ["localhost:9001", "localhost:9002"] },
		  { ChunkID: "sample.txt_chunk_001", Locations: ["localhost:9001", "localhost:9002"] },
		]
	*/

	return placement
}

func sendChunkToNode(nodeAddr, chunkID string, data []byte) error {
	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	writer := bufio.NewWriter(conn)
	// Send header: CHUNK|chunkID|size\n
	header := fmt.Sprintf("CHUNK|%s|%d\n", chunkID, len(data))
	if _, err := writer.WriteString(header); err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	return writer.Flush()
}

func registerChunksWithMaster(filename string, placements []ChunkInfo) {
	conn, err := net.Dial("tcp", ":9100")
	if err != nil {
		fmt.Println("Error connecting to master:", err)
		return
	}
	defer conn.Close()
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	req := map[string]interface{}{
		"type":     "register_chunks",
		"filename": filename,
		"chunks":   placements,
	}
	encoder.Encode(req)
	var resp map[string]string
	decoder.Decode(&resp)
	fmt.Println("Master response:", resp)
}

// func sendChunkToNode(nodeAddr, chunkID string, chunkData []byte) error {
// 	conn, err := net.Dial("tcp", nodeAddr)
// 	if err != nil {
// 		return fmt.Errorf("error connecting to node %s: %v", nodeAddr, err)
// 	}
// 	defer conn.Close()

// 	// Prepare the chunk data
// 	chunk := map[string]interface{}{
// 		"type":     "register_chunks",
// 		"filename": "sample.txt",
// 		"chunk_id": chunkID,
// 		"data":     chunkData,
// 	}

// 	// Send the chunk data
// 	encoder := json.NewEncoder(conn)
// 	if err := encoder.Encode(chunk); err != nil {
// 		return fmt.Errorf("error encoding chunk data: %v", err)
// 	}

// 	return nil
// }
