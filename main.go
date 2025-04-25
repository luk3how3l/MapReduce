package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"mapreducez"
)

var localaddress string

// node types
const (
	MASTER_NODE = "master"
	WORKER_NODE = "worker"
)

type Node struct {
	Address    string
	NodeType   string
	Workers    []string              // only used by master to track worker addresses
	MasterAddr string               // only used by workers to track their master
	MapReduce  *mapreducez.Master   // master node's MapReduce instance
	Worker     *mapreducez.Worker   // worker node's Worker instance
}

// find our local ip address
func init() {
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

// start server based on node type (master or worker)
func StartServer(address string, masterAddr string) (*Node, error) {
	node := &Node{
		Address: address,
	}

	if masterAddr == "" {
		// Initialize as master node
		node.NodeType = MASTER_NODE
		node.Workers = make([]string, 0)
		
		// Create MapReduce master instance
		master := &mapreducez.Master{
			Workers: make(map[string]*mapreducez.Worker),
			MapTasks: make(map[int]*mapreducez.MapTask),
			ReduceTasks: make(map[int]*mapreducez.ReduceTask),
		}
		node.MapReduce = master
		
		log.Printf("starting master node at %s", address)
	} else {
		// Initialize as worker node
		node.NodeType = WORKER_NODE
		node.MasterAddr = masterAddr
		
		// Create worker instance
		worker := &mapreducez.Worker{}
		node.Worker = worker
		
		log.Printf("starting worker node at %s, connecting to master at %s", address, masterAddr)
		
		// Register with master
		if err := registerWithMaster(node); err != nil {
			return nil, fmt.Errorf("failed to register with master: %v", err)
		}
	}

	return node, nil
}

// register worker with master node
func registerWithMaster(node *Node) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Connect to master's RPC server
	conn, err := net.Dial("tcp", node.MasterAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to master: %v", err)
	}
	defer conn.Close()

	// Register worker with master
	client := mapreducez.NewRPCClient(conn)
	err = client.Call("Master.RegisterWorker", node.Address, nil)
	if err != nil {
		return fmt.Errorf("failed to register with master: %v", err)
	}

	log.Printf("successfully registered with master at %s", node.MasterAddr)
	return nil
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

		switch parts[0] {
		case "help":
			printHelp(node.NodeType)

		case "status":
			if node.NodeType == MASTER_NODE {
				fmt.Printf("Master node at %s\n", node.Address)
				fmt.Printf("Connected workers: %v\n", node.Workers)
			} else {
				fmt.Printf("Worker node at %s\n", node.Address)
				fmt.Printf("Connected to master: %s\n", node.MasterAddr)
			}

		case "start":
			if node.NodeType != MASTER_NODE {
				fmt.Println("Only master node can start MapReduce jobs")
				continue
			}
			if len(parts) < 3 {
				fmt.Println("Usage: start <input_file> <num_reduce_tasks>")
				continue
			}
			
			inputFile := parts[1]
			numReduceTasks, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Printf("Invalid number of reduce tasks: %v\n", err)
				continue
			}

			err = node.MapReduce.MasterMain(inputFile, numReduceTasks)
			if err != nil {
				fmt.Printf("MapReduce job failed: %v\n", err)
			} else {
				fmt.Println("MapReduce job completed successfully")
			}

		case "quit":
			if node.NodeType == MASTER_NODE {
				// Notify all workers to shut down
				for _, workerAddr := range node.Workers {
					notifyWorkerShutdown(workerAddr)
				}
			}
			return

		default:
			fmt.Println("Unknown command. Type 'help' for available commands.")
		}
	}
}

func printHelp(nodeType string) {
	fmt.Println("Available commands:")
	fmt.Println("  help              - Show this help message")
	fmt.Println("  status            - Show node status")
	if nodeType == MASTER_NODE {
		fmt.Println("  start <input> <R>  - Start MapReduce job with input file and R reduce tasks")
	}
	fmt.Println("  quit              - Exit the program")
}

func notifyWorkerShutdown(workerAddr string) {
	conn, err := net.Dial("tcp", workerAddr)
	if err != nil {
		log.Printf("Failed to connect to worker %s for shutdown: %v", workerAddr, err)
		return
	}
	defer conn.Close()

	client := mapreducez.NewRPCClient(conn)
	err = client.Call("Worker.Shutdown", struct{}{}, nil)
	if err != nil {
		log.Printf("Failed to notify worker %s for shutdown: %v", workerAddr, err)
	}
}

func main() {
	// Parse command line flags
	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	createPort := createCmd.Int("port", 3410, "Port to listen on")

	joinCmd := flag.NewFlagSet("join", flag.ExitOnError)
	joinPort := joinCmd.Int("port", 3410, "Port to listen on")
	masterAddr := joinCmd.String("master", "", "Address of master node")

	if len(os.Args) < 2 {
		fmt.Println("Expected 'create' (for master) or 'join' (for worker) subcommand")
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
		node, err = StartServer(address, "") // Empty master address means this is master
		if err != nil {
			log.Fatalf("Failed to create master node: %v", err)
		}
		log.Printf("Created master node at %s", node.Address)

	case "join":
		err := joinCmd.Parse(os.Args[2:])
		if err != nil {
			log.Fatal(err)
		}

		if *masterAddr == "" {
			log.Fatal("Worker nodes require master address (-master flag)")
		}

		address = fmt.Sprintf(":%d", *joinPort)
		node, err = StartServer(address, *masterAddr)
		if err != nil {
			log.Fatalf("Failed to create worker node: %v", err)
		}
		log.Printf("Created worker node at %s", node.Address)

	default:
		fmt.Println("Expected 'create' (for master) or 'join' (for worker) subcommand")
		os.Exit(1)
	}

	// Run the interactive shell
	RunShell(node)
}
