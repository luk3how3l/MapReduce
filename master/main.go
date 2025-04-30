package main

import (
	"log"
	"net"
	"sync"

	"github.com/your-project/mapreducez"
)

type Master struct {
	Workers []*mapreducez.Worker
	mu      sync.Mutex
}

func (m *Master) RegisterWorker(worker *mapreducez.Worker, reply *struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	log.Printf("Registering new worker: %s", worker.Addr)
	m.Workers = append(m.Workers, worker)
	return nil
}

func main() {
	// Create master instance
	master := &Master{
		Workers: make([]*mapreducez.Worker, 0),
	}

	// Start RPC server
	server := mapreducez.NewRPCServer()
	server.Register("Master", master)

	// Start listening for worker connections
	listener, err := net.Listen("tcp", ":3410")
	if err != nil {
		log.Fatalf("Failed to start RPC server: %v", err)
	}
	defer listener.Close()

	log.Printf("Master RPC server listening on %s", listener.Addr().String())

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go server.ServeConn(conn)
	}
} 