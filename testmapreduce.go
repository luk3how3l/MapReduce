package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

/*
Goals

map task processed 8423 pairs, generated 85153 pairs
reduce task processed 6460 keys and 234249 values, generated 6460 pairs
*/
func mainm() {
	runtime.GOMAXPROCS(1)
	fmt.Println("Runs main on testmapreduce")

	m, r := 9, 3
	source := "source.db"
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

	// Start HTTP server for data files
	myAddress := net.JoinHostPort(getLocalAddress(), "3410")
	log.Printf("starting HTTP server at %s", myAddress)
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))

	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("Listen error on address %s: %v", myAddress, err)
	}
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("Serve error: %v", err)
		}
	}()

	// Create MapTasks
	var mapTasks []*MapTask
	for i := 0; i < m; i++ {
		mapTasks = append(mapTasks, &MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: myAddress,
		})
	}

	// Create ReduceTasks
	var reduceTasks []*ReduceTask
	for i := 0; i < r; i++ {
		reduceTasks = append(reduceTasks, &ReduceTask{
			M:           m,
			R:           r,
			N:           i,
			SourceHosts: make([]string, m),
		})
	}

	client := Client{}

	// Process map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing map task %d: %v", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = myAddress
		}
	}

	// Process reduce tasks
	for i, task := range reduceTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing reduce task %d: %v", i, err)
		}
	}

	// Merge outputs
	//var rr []string
	//rr = append(rr, r)
	dbPath, err := mergeDatabases(paths, tempdir, "output.txt")
	if err != nil {
		log.Fatalf("merging outputs failed: %v", err)
	}
	log.Printf("Merged database created at: %s", dbPath)

	//openDatabase(dbPath)
	log.Println("MapReduce completed successfully.")
}
