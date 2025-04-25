package mapreducez

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	petname "github.com/dustinkirkland/golang-petname"

	pb "chord/protocol" // Update path as needed

	"google.golang.org/grpc"
)

var localaddress string

// Find our local IP address
func init() {
	// Configure log package to show short filename, line number and timestamp with only time
	log.SetFlags(log.Lshortfile | log.Ltime)

	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	localaddress = localAddr.IP.String()

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	log.Printf("found local address %s\n", localaddress)
}

// resolveAddress handles :port format by adding the local address
func resolveAddress(address string) string {
	if strings.HasPrefix(address, ":") {
		return net.JoinHostPort(localaddress, address[1:])
	} else if !strings.Contains(address, ":") {
		return net.JoinHostPort(address, defaultPort)
	}
	return address
}

// StartServer starts the gRPC server for this node
func StartServer(address string, nprime string) (*Node, error) {
	address = resolveAddress(address)

	node := &Node{
		Address:     address,
		
	}

	// Are we the first node?
	if nprime == "" {
		log.Print("StartServer: creating new Master")
		node.Successors = []string{node.Address}
	} else {
		log.Print("StartServer: joining existing ring using ", nprime)
		// For now use the given address as our successor
		nprime = resolveAddress(nprime)
		node.Successors = []string{nprime}

		// TODO: use a GetAll request to populate our bucket
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		client, err := grpc.Dial(nprime, grpc.WithInsecure()) // Connect to successor
		if err != nil {
			log.Fatalf("StartServer: Failed to connect to successor %s: %v", nprime, err)
		}
		defer client.Close()

		chordClient := pb.NewChordClient(client)
		getSomeReq := &pb.GetSomeRequest{NewPredecessorAddress: node.Address}

		getSomeResp, err := chordClient.GetSome(ctx, getSomeReq)
		if err != nil {
			log.Printf("StartServer: Failed to get key-value pairs from successor: %v", err)
		} else {
			node.Bucket = getSomeResp.KeyValues // Store received key-value pairs
			log.Printf("StartServer: Populated bucket with %d key-value pairs", len(node.Bucket))
		}
	}

	// Start listening for RPC calls
	grpcServer := grpc.NewServer()
	pb.RegisterChordServer(grpcServer, node)

	lis, err := net.Listen("tcp", node.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	// Start server in goroutine
	log.Printf("Starting Chord node server on %s", node.Address)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Start background tasks
	go func() {
		// ping every 1 second
		for {
			time.Sleep(time.Second / 3)
			node.stabilize()

			time.Sleep(time.Second / 3)
			nextFinger = node.fixFingers(nextFinger)

			time.Sleep(time.Second / 3)
			node.checkPredecessor()
		}
	}()

	return node, nil
}

// RunShell provides an interactive command shell
func RunShell(node *Node) {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("\nExiting...")
				return
			}
			fmt.Println("Error reading input:", err)
			continue
		}

		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		switch parts[0] {
		case "help":
			fmt.Println("Available commands:")
			fmt.Println("  help              - Show this help message")
			fmt.Println("  ping <address>    - Ping another node")
			fmt.Println("                      (You can use :port for localhost)")
			fmt.Println("  putalot 			=- cheat is put me : to auto fill")
			fmt.Println("  put <key> <value>  - Store a key-value pair on a node")
			fmt.Println("  get <key>          - Get a value for a key from a node")
			fmt.Println("  delete <key>       - Delete a key from a node")
			fmt.Println("  getall <address>            - Get all key-value pairs from a node")
			fmt.Println("  dump              - Display info about the current node")
			fmt.Println("  quit              - Exit the program")

		case "ping":
			if len(parts) < 2 {
				fmt.Println("Usage: ping <address>")
				continue
			}

			err := PingNode(ctx, parts[1])
			if err != nil {
				fmt.Printf("Ping failed: %v\n", err)
			} else {
				fmt.Println("Ping successful")
			}

		case "put":
			if len(parts) < 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}

			target, err := find(hash(parts[1]), node.Address)
			if err != nil {
				fmt.Printf("Find failed: %v\n", err)
				continue
			}

			err = PutKeyValue(ctx, parts[1], parts[2], target)
			if err != nil {
				fmt.Printf("Put failed: %v\n", err)
			} else {
				fmt.Printf("Put successful: %s -> %s\n", parts[1], parts[2])
			}
		case "putalot":
			if len(parts) < 1 {
				fmt.Println("Usage: putalot")
				continue
			}

			rand.Seed(time.Now().UnixNano()) // Seed the random number generator
			words := make([]string, 100)

			for i := range words {
				words[i] = petname.Generate(1, "-") // Generates a single random word
			}
			knumbers := make([]int, 100) // Create a slice for 50 numbers

			// Generate random numbers between 1000 and 99999 (to ensure 4 or 5 digits)
			for i := range knumbers {
				knumbers[i] = rand.Intn(90000) + 1000
			}
			//fmt.Printf("the list %v", knumbers)

			for k := 0; k < len(knumbers); k++ {

				target, err := find(hash(words[k]), node.Address)
				if err != nil {
					fmt.Printf("Find failed: %v\n", err)
					continue
				}

				errr := PutKeyValue(ctx, words[k], strconv.Itoa(knumbers[k]), target)
				if errr != nil {
					fmt.Printf("Put failed: %v\n", errr)
				} else {
					fmt.Printf("Put successful: %s -> %s\n", words[k], strconv.Itoa(knumbers[k]))
				}
			}

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}

			target, err := find(hash(parts[1]), node.Address)
			if err != nil {
				fmt.Printf("Find failed: %v\n", err)
				continue
			}

			value, err := GetValue(ctx, parts[1], target)
			if err != nil {
				fmt.Printf("Get failed: %v\n", err)
			} else if value == "" {
				fmt.Printf("Key '%s' not found\n", parts[1])
			} else {
				fmt.Printf("%s -> %s\n", parts[1], value)
			}

		case "delete":
			if len(parts) < 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}

			target, err := find(hash(parts[1]), node.Address)
			if err != nil {
				fmt.Printf("Find failed: %v\n", err)
				continue
			}

			err = DeleteKey(ctx, parts[1], target)
			if err != nil {
				fmt.Printf("Delete failed: %v\n", err)
			} else {
				fmt.Printf("Delete request for key '%s' completed\n", parts[1])
			}

		case "getall":
			if len(parts) < 2 {
				fmt.Println("Usage: getall <address>")
				continue
			}

			keyValues, err := GetAllKeyValues(ctx, parts[1])
			if err != nil {
				fmt.Printf("GetAll failed: %v\n", err)
			} else {
				if len(keyValues) == 0 {
					fmt.Println("No key-value pairs found")
				} else {
					fmt.Println("Key-value pairs:")
					for k, v := range keyValues {
						fmt.Printf("  %s -> %s\n", k, v)
					}
				}
			}

		case "dump":
			node.dump()

		case "quit":
			fmt.Println("Exiting...")
			//TODO: add putall here to give nodes to sucessor before quitting
			successor := node.Successors[0]
			log.Printf("Leave: Transferring %d key-value pairs to successor %s", len(node.Bucket), successor)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// Call PutAll on the successor
			conn, err := grpc.Dial(successor, grpc.WithInsecure())
			if err != nil {
				log.Printf("Leave: Failed to connect to successor %s: %v", successor, err)
				return
			}

			client := pb.NewChordClient(conn)

			_, err = client.PutAll(ctx, &pb.PutAllRequest{KeyValues: node.Bucket})
			if err != nil {
				log.Printf("Leave: Failed to transfer data to successor %s: %v", successor, err)
				return
			}

			log.Println("Leave: Data successfully transferred, shutting down.")

			return

		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

func main() {
	// Parse command line flags
	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	createPort := createCmd.Int("port", 3410, "Port to listen on")

	joinCmd := flag.NewFlagSet("join", flag.ExitOnError)
	joinPort := joinCmd.Int("port", 3410, "Port to listen on")
	joinAddr := joinCmd.String("addr", "", "Address of existing node")

	if len(os.Args) < 2 {
		fmt.Println("Expected 'create' or 'join' subcommand")
		os.Exit(1)
	}

	var node *Node
	var address string

	switch os.Args[1] {
	case "create":
		err := createCmd.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}

		address = fmt.Sprintf(":%d", *createPort)
		node, err = StartServer(address, "")
		if err != nil {
			log.Fatalf("Failed to create node: %v", err)
		}
		log.Printf("Created new ring with node at %s", node.Address)

	case "join":
		err := joinCmd.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}

		if *joinAddr == "" {
			log.Fatal("Join requires an address of an existing node")
		}

		address = fmt.Sprintf(":%d", *joinPort)
		node, err = StartServer(address, *joinAddr)
		if err != nil {
			log.Fatalf("Failed to join ring: %v", err)
		}
		log.Printf("Joined ring with node at %s", node.Address)

	default:
		fmt.Println("Expected 'create' or 'join' subcommand")
		os.Exit(1)
	}

	// Run the interactive shell
	RunShell(node)
}
