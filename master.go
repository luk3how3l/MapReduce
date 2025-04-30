package mapreducez

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
)

/* 
master metadata needs:
1. number of map tasks
2. number of reduce tasks
3. bool for whether an worker is idle
4. an list of int for each (map or reduce) task (0, not done, 1 working, 2 done) (the index is which task)
5. list of links to the tasks (the index is which task)
master stores the metadata of location and size of r intermediate files
updates the metadata as map task is complete. 
*/
const (
	numMapTasks = 10 // Default number of map tasks
)

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

type Master struct {
	Mu           sync.Mutex
	Workers      map[string]*Worker
	MapTasks     map[int]*MapTask
	ReduceTasks  map[int]*ReduceTask
	Client       Interface
}

/*
mapreduce master:

1. initialize the master
2. register the master with the rpc server
3. start the rpc server
4. split the source file into m pieces (does m pieces mean 16-64 mb?)
5. wait for the worker to register
6. master pings the workers to see if alive. 	
7. assign the map tasks to the worker
8. wait for the worker to finish the tasks
9. return the results to the client

when everything is finishing up an master reassign map tasks if there is an tail end latency.
*/

// master pseudocode

func (m *Master) MasterMain(inputFile string, numReduceTasks int) error {
	log.Printf("Starting MapReduce job with input file %s and %d reduce tasks", inputFile, numReduceTasks)
	
	// === initialization ===
	// create temp directory for intermediate files
	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	if err := os.MkdirAll(tempdir, 0700); err != nil {
		return fmt.Errorf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// split input file into m chunks
	paths := make([]string, numMapTasks)
	for i := 0; i < numMapTasks; i++ {
		paths[i] = filepath.Join(tempdir, mapSourceFile(i))
	}
	if err := splitDatabase(inputFile, paths); err != nil {
		return fmt.Errorf("failed to split input: %v", err)
	}

	// === start http server ===
	// start http server to serve map input chunks
	myAddress := net.JoinHostPort(getLocalAddress(), "3410")
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		return fmt.Errorf("failed to start http server: %v", err)
	}
	go http.Serve(listener, nil)

	// === generate tasks ===
	// create map tasks
	for i := 0; i < numMapTasks; i++ {
		m.MapTasks[i] = &MapTask{
			M: numMapTasks,
			R: numReduceTasks,
			N: i,
			SourceHost: myAddress,
		}
	}

	// create reduce tasks
	for i := 0; i < numReduceTasks; i++ {
		m.ReduceTasks[i] = &ReduceTask{
			M: numMapTasks,
			R: numReduceTasks,
			N: i,
			SourceHosts: make([]string, numMapTasks),
		}
	}

	// === map phase ===
	var mapWG sync.WaitGroup
	for id, task := range m.MapTasks {
		mapWG.Add(1)
		go func(taskID int, mapTask *MapTask) {
			defer mapWG.Done()
			
			// assign task to available worker
			worker := m.getIdleWorker()
			if err := worker.Process(tempdir, mapTask, m.Client); err != nil {
				log.Printf("map task %d failed: %v", taskID, err)
				// handle failure - possibly reassign task
				return
			}

			// update reduce tasks with this map task's output location
			m.Mu.Lock()
			for _, reduceTask := range m.ReduceTasks {
				reduceTask.SourceHosts[taskID] = worker.Address
			}
			m.Mu.Unlock()
		}(id, task)
	}
	mapWG.Wait()

	// === reduce phase ===
	var reduceWG sync.WaitGroup
	for id, task := range m.ReduceTasks {
		reduceWG.Add(1)
		go func(taskID int, reduceTask *ReduceTask) {
			defer reduceWG.Done()

			worker := m.getIdleWorker()
			if err := worker.Process(tempdir, reduceTask, m.Client); err != nil {
				log.Printf("reduce task %d failed: %v", taskID, err)
				// handle failure - possibly reassign task
				return
			}
		}(id, task)
	}
	reduceWG.Wait()

	// === final output ===
	// gather reduce outputs
	var reduceOutputPaths []string
	for i := 0; i < numReduceTasks; i++ {
		reduceOutputPaths = append(reduceOutputPaths, makeURL(myAddress, reduceOutputFile(i)))
	}

	// merge final outputs
	outputPath := filepath.Join(".", "output.db")
	tempOutput := filepath.Join(tempdir, "output-tmp.db")
	if _, err := mergeDatabases(reduceOutputPaths, outputPath, tempOutput); err != nil {
		return fmt.Errorf("failed to merge outputs: %v", err)
	}

	// cleanup
	m.notifyWorkersShutdown()
	return nil
}

// helper methods for master
func (m *Master) getIdleWorker() *Worker {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	
	for _, worker := range m.Workers {
		if worker.IsIdle {
			worker.IsIdle = false
			return worker
		}
	}
	return nil
}

func (m *Master) notifyWorkersShutdown() {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	
	for _, worker := range m.Workers {
		// TODO: Implement worker shutdown notification
		log.Printf("Notifying worker %s to shutdown", worker.Address)
	}
}
