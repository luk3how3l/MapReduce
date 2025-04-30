package mapreducez

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net"
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

type Pair struct {
	Key   string
	Value string
}

type KeyGroup struct {
	key    string
	values <-chan string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

type Worker struct {
	Address string
	IsIdle  bool
	done    chan struct{}
}

func NewWorker(address string) *Worker {
	return &Worker{
		Address: address,
		IsIdle:  true,
		done:    make(chan struct{}),
	}
}

func (w *Worker) Process(tempdir string, task interface{}, client Interface) error {
	switch t := task.(type) {
	case *MapTask:
		return t.Process(tempdir, client)
	case *ReduceTask:
		return t.Process(tempdir, client)
	default:
		return fmt.Errorf("unknown task type: %T", task)
	}
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

func groupby(rawPairs <-chan Pair) <-chan KeyGroup {
	// Create the output channel for KeyGroups
	groups := make(chan KeyGroup)

	// Start a goroutine to process the incoming pairs
	go func() {
		// Ensure the groups channel gets closed when we're done
		defer close(groups)

		var currentKey string
		var valuesChan chan string

		for pair := range rawPairs {
			// If this is a new key or the first key
			if pair.Key != currentKey || valuesChan == nil {
				// Close the previous values channel if it exists
				if valuesChan != nil {
					close(valuesChan)
				}

				// Create a new values channel for this key
				valuesChan = make(chan string)

				// Update the current key
				currentKey = pair.Key

				// Send a new KeyGroup with this key and values channel
				groups <- KeyGroup{
					key:    currentKey,
					values: valuesChan,
				}
			}

			// Send the value to the current values channel
			valuesChan <- pair.Value
		}

		// Close the final values channel when rawPairs is closed
		if valuesChan != nil {
			close(valuesChan)
		}
	}()

	return groups
}

func (task *MapTask) Process(tempdir string, client Interface) error {
	//use mapInputFile to get the name of the file based on the task number
	inputFile := mapSourceFile(task.N)
	url := makeURL(task.SourceHost, inputFile)
	inputPath := filepath.Join(tempdir, mapInputFile(task.N))

	//Downloads file from master
	if err := download(url, inputPath); err != nil {
		return fmt.Errorf("failed to download map source file: %v", err)
	}

	//opens downloaded file
	db, err := openDatabase(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer db.Close()

	//Creating output files
	outputDBs := make([]*sql.DB, task.R)
	for i := 0; i < task.R; i++ {
		outputPath := filepath.Join(tempdir, mapOutputFile(task.N, i))
		outputDB, err := createDatabase(outputPath)
		if err != nil {
			return fmt.Errorf("failed to open output database %s: %v", outputPath, err)
		}
		outputDBs[i] = outputDB
		defer outputDB.Close()
	}

	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return fmt.Errorf("error querying database: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		//gets next key value pair
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("error scanning row value: %v", err)
		}

		outputChan := make(chan Pair, 100)
		imgood := make(chan bool, 100)

		// Background goroutine to insert pairs into corresponding db by read
		go func() {
			for pair := range outputChan {
				hasher := fnv.New32()
				hasher.Write([]byte(pair.Key))
				r := int(hasher.Sum32() % uint32(task.R))

				_, err := outputDBs[r].Exec("INSERT INTO pairs (key, value) VALUES (?, ?)", pair.Key, pair.Value)
				if err != nil {
					log.Printf("failed to insert pair (%s, %s): %v", pair.Key, pair.Value, err)
				}
			}
			//signaling it's done writing
			imgood <- true
		}()

		// Call client.map on each key value pair/ it's a write
		go func(k, v string) {
			err := client.Map(k, v, outputChan)
			if err != nil {
				log.Printf("client.Map error on key=%s: %v", k, err)
			}
		}(key, value)
		<-imgood //the magic <-
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating through rows: %v", err)
	}
	log.Printf("Processed task %v\n", task.N)
	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	var url []string
	tempPath := filepath.Join(tempdir, reduceTempFile(task.N))
	inputPath := filepath.Join(tempdir, reduceInputFile(task.N))
	outputPath := filepath.Join(tempdir, reduceOutputFile(task.N))

	// Build URLs for map output files
	for i := 0; i < task.M; i++ {
		mapOutputLoc := mapOutputFile(i, task.N)
		url = append(url, makeURL(task.SourceHosts[i], mapOutputLoc))
	}

	// Download and merge map outputs
	if _, err := mergeDatabases(url, inputPath, tempPath); err != nil {
		return fmt.Errorf("merge failed: %v", err)
	}

	// Create and open output DB
	if _, err := createDatabase(outputPath); err != nil {
		return fmt.Errorf("create output db failed: %v", err)
	}
	outputDB, err := sql.Open("sqlite3", outputPath)
	if err != nil {
		return fmt.Errorf("failed to open output db: %v", err)
	}
	defer outputDB.Close()

	// Open input DB and read rows
	db, err := openDatabase(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input db: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT key, value FROM pairs ORDER BY key, value")
	if err != nil {
		return fmt.Errorf("query error: %v", err)
	}
	defer rows.Close()

	// Pipe SQL rows into rawPairs channel
	rawPairs := make(chan Pair, 100)
	done := make(chan bool, 100)

	go func() {
		defer close(rawPairs)
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				log.Printf("row scan error: %v", err)
				continue
			}
			rawPairs <- Pair{Key: k, Value: v}
		}
	}()

	// Group by key and process each group
	for keyGroup := range groupby(rawPairs) {
		outputChan := make(chan Pair, 100)
		// Launch writer goroutine
		go func(key string, out <-chan Pair) {
			for pair := range out {
				_, err := outputDB.Exec(`INSERT INTO pairs (key, value) VALUES (?, ?)`, pair.Key, pair.Value)
				if err != nil {
					log.Printf("insert error for key=%s: %v", pair.Key, err)
				}
			}
			done <- true
		}(keyGroup.key, outputChan)

		// Run reduce in-place (can be goroutine if needed)
		go func() {
			err := client.Reduce(keyGroup.key, keyGroup.values, outputChan)
			if err != nil {
				log.Printf("Reduce error on key=%s: %v", keyGroup.key, err)
			}
		}()
		<-done
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("row iteration error: %v", err)
	}

	log.Printf("Processed reduce task %v\n", task.N)
	return nil
}


/*
func main() {
	fmt.Println("Runs main")
	m := 10
	r := 5
	source := "source.db"
	target := "target.db"
	tmp := os.TempDir()
	runtime.GOMAXPROCS(1)

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
	// this takes a tcp connection and converts it into a http request
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
	log.Printf("MapTask process complete, Processing %d reduce tasks\n", r)

	// process the reduce tasks
	for i, task := range reduceTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing reduce task %d: %v", i, err)
		}
	}
	log.Printf("ReduceTask process complete\n")

	// gather outputs into final target.db file
	var reduceOutputPaths []string
	for i := 0; i < r; i++ {
		reduceOutputPaths = append(reduceOutputPaths, makeURL(myAddress, reduceOutputFile(i)))
	}

	targetPath := filepath.Join(".", target)
	tempTarget := filepath.Join(tempdir, "target-tmp.db")

	outputDB, err := mergeDatabases(reduceOutputPaths, targetPath, tempTarget)
	if err != nil {
		log.Fatalf("failed to merge reduce outputs into target.db: %v", err)
	}
	defer outputDB.Close()

	log.Printf("Successfully wrote final output to %s", targetPath)
}
*/