package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

type ChunkInfo struct {
	ChunkID   string   `json:"chunk_id"`
	Locations []string `json:"locations"` // Basically Node Address
}

type FileMetadata struct {
	Filename string      `json:"filename"`
	Chunks   []ChunkInfo `json:"chunks"`
}

type Master struct {
	mu         sync.Mutex
	fileChunks map[string][]string // filename -> []chunkID
	chunkNodes map[string][]string // chunkID -> []nodeAddr
	nodes      []string            // List of all node address
}

func NewMaster(nodes []string) *Master {
	return &Master{
		fileChunks: make(map[string][]string),
		chunkNodes: make(map[string][]string),
		nodes:      nodes,
	}
}

/*
*
Handle the client request for chunkplacement and metadata updates
*/
func (m *Master) handleConnection(conn net.Conn) {
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	var req map[string]interface{}
	if err := decoder.Decode(&req); err != nil {
		fmt.Println("Error in decoding the request:", err)
		return
	}
	// fmt.Println("Req :\n", req)

	switch req["type"] {
	case "get_chunk_placement":
		// Request: {type: "get_chunk_placement", filename: "...", num_chunks: N}
		filename := req["filename"].(string)
		numChunks := int(req["num_chunks"].(float64))
		placements := m.assignChunks(filename, numChunks)
		encoder.Encode(placements)

	case "register_chunks":
		// fmt.Println(req)
		filename := req["filename"].(string)
		chunks := req["chunks"].([]interface{})
		m.mu.Lock()
		for _, c := range chunks {
			chunks := c.(map[string]interface{})
			chunkID := chunks["chunk_id"].(string)
			locs := []string{}
			for _, l := range chunks["locations"].([]interface{}) {
				locs = append(locs, l.(string))
			}
			m.fileChunks[filename] = append(m.fileChunks[filename], chunkID)
			m.chunkNodes[chunkID] = locs
			encoder.Encode(map[string]string{"status": "ok"})
		}
		m.mu.Unlock()
	}

	fmt.Println(m.fileChunks)
	fmt.Println(m.chunkNodes)

}

/*
Assign chunks to nodes for a given file
This function generates chunk IDs and assigns them to all nodes.
Returns a slice of ChunkInfo which contains the chunk ID and the list of node addresses where the chunk is stored.
*/
func (m *Master) assignChunks(filename string, numChunks int) []ChunkInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	placement := []ChunkInfo{}
	for i := 0; i < numChunks; i++ {
		chunkID := fmt.Sprintf("%s_chunk_%03d", filename, i)

		locationAddress := make([]string, len(m.nodes))
		copy(locationAddress, m.nodes)

		placement = append(placement, ChunkInfo{ChunkID: chunkID, Locations: locationAddress})
	}
	return placement
}

func main() {
	// List of Node Adress ( HOST:PORT)
	nodes := []string{"localhost:9001", "localhost:9002", "localhost:9003"}

	master := NewMaster(nodes)

	listner, err := net.Listen("tcp", ":9100")
	if err != nil {
		fmt.Println("Error starting master", err)
		os.Exit(1)
	}
	fmt.Println("Master listening on port 9100")
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
		go master.handleConnection(conn)
	}

}
