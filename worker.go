package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	_ "github.com/mattn/go-sqlite3"
)

/* worker notes
A worker node needs to do the following in a loop:

Start an HTTP server to serve intermediate data files to other workers and back to the master.
Request a job from the master.
Process each job using the code from part 2.
Shut down and clean up when finished.
*/

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	localaddress := localAddr.IP.String()

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localaddress
}

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

func (task *MapTask) Process(tempdir string, client Interface) error {
	inputFile := mapInputFile(task.N)
	url := makeURL(task.SourceHost, inputFile)
	inputPath := filepath.Join(tempdir, inputFile)

	if err := download(url, inputPath); err != nil {
		return fmt.Errorf("failed to download map source file: %v", err)
	}

	db, err := openDatabase(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer db.Close()

	outputDBs := make([]*sql.DB, task.R)
	for i := 0; i < task.R; i++ {
		outputPath := filepath.Join(tempdir, mapOutputFile(task.N, i))
		outputDBs[i], err = sql.Open("sqlite3", outputPath)
		if err != nil {
			return fmt.Errorf("failed to open output database %s: %v", outputPath, err)
		}
		defer outputDBs[i].Close()
	}

	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("error scanning row value: %v", err)
		}

		outputChan := make(chan Pair)
		go func() {
			client.Map(key, value, outputChan)
			close(outputChan)
		}()

		for pair := range outputChan {
			hasher := fnv.New32()
			hasher.Write([]byte(pair.Key))
			r := int(hasher.Sum32() % uint32(task.R))

			_, err := outputDBs[r].Exec("INSERT INTO pairs (key, value) VALUES (?, ?)", pair.Key, pair.Value)
			if err != nil {
				return fmt.Errorf("failed to insert pair into output database: %v", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating through rows: %v", err)
	}

	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	return nil
}

func main() {
	fmt.Println("Runs main")
	m := 10
	r := 5
	source := "source.db"
	//target := "target.db"
	tmp := os.TempDir()

	tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))
	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to delete old temp dir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		log.Fatalf("unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	log.Printf("splitting %s into %d pieces", source, m)
	var paths []string
	for i := 0; i < m; i++ {
		paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
	}
	if err := splitDatabase(source, paths); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	myAddress := net.JoinHostPort(getLocalAddress(), "3410")
	log.Printf("starting http server at %s", myAddress)
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))

	// bind on the port before launching the background goroutine on Serve
	// to prevent race condition with call to download below
	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("Listen error on address %s: %v", myAddress, err)
	}
	//I think that this takes a tcp connection and converts it into a http request
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("Serve error: %v", err)
		}
	}()

	// build the map tasks
	var mapTasks []*MapTask
	for i := 0; i < m; i++ {
		task := &MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: myAddress,
		}
		mapTasks = append(mapTasks, task)
	}

	// build the reduce tasks
	var reduceTasks []*ReduceTask
	for i := 0; i < r; i++ {
		task := &ReduceTask{
			M:           m,
			R:           r,
			N:           i,
			SourceHosts: make([]string, m),
		}
		reduceTasks = append(reduceTasks, task)
	}

	var client Client

	// process the map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing map task %d: %v", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = myAddress
		}
	}

	// process the reduce tasks
	for i, task := range reduceTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing reduce task %d: %v", i, err)
		}
	}

	// gather outputs into final target.db file
}
