package mapreducez

import (
	"encoding/gob"
	"log"
	"net"
)

// RPCClient represents a client for making RPC calls
type RPCClient struct {
	conn net.Conn
	enc  *gob.Encoder
	dec  *gob.Decoder
}

// NewRPCClient creates a new RPC client
func NewRPCClient(conn net.Conn) *RPCClient {
	return &RPCClient{
		conn: conn,
		enc:  gob.NewEncoder(conn),
		dec:  gob.NewDecoder(conn),
	}
}

// Call makes an RPC call to the server
func (c *RPCClient) Call(method string, args interface{}, reply interface{}) error {
	// Send the method name and arguments
	if err := c.enc.Encode(method); err != nil {
		return err
	}
	if err := c.enc.Encode(args); err != nil {
		return err
	}

	// Receive the reply
	return c.dec.Decode(reply)
}

// RPCServer represents an RPC server
type RPCServer struct {
	handlers map[string]func(interface{}, interface{}) error
}

// NewRPCServer creates a new RPC server
func NewRPCServer() *RPCServer {
	return &RPCServer{
		handlers: make(map[string]func(interface{}, interface{}) error),
	}
}

// Register registers a handler for a method
func (s *RPCServer) Register(method string, handler func(interface{}, interface{}) error) {
	s.handlers[method] = handler
}

// ServeConn serves a single connection
func (s *RPCServer) ServeConn(conn net.Conn) {
	defer conn.Close()

	enc := gob.NewEncoder(conn)
	dec := gob.NewDecoder(conn)

	for {
		var method string
		if err := dec.Decode(&method); err != nil {
			log.Printf("Error decoding method: %v", err)
			return
		}

		handler, ok := s.handlers[method]
		if !ok {
			log.Printf("Unknown method: %s", method)
			return
		}

		var args interface{}
		if err := dec.Decode(&args); err != nil {
			log.Printf("Error decoding args: %v", err)
			return
		}

		var reply interface{}
		if err := handler(args, &reply); err != nil {
			log.Printf("Error handling %s: %v", method, err)
			return
		}

		if err := enc.Encode(reply); err != nil {
			log.Printf("Error encoding reply: %v", err)
			return
		}
	}
} 