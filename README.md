# MapReduce
CS3410 distributed system 
official instruction:
https://www.cs.utahtech.edu/cs/3410/asst_mapreduce.html

PArt 1 is actually part 2. we write code in Worker.go file

MapTask.Process
ReduceTask.Process

Part 2: 
implentment- in master.go file 

-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=--
Setup:
 GO get github.com/mattn/go-sqlite3
Go build 
Go init

Goal for Part 1:
    Get Map Task -DONE
    Map Handler, Reduce Handler
    MapTask: (should download select with SQL?)
    ReduceTask needs to be finished.

    We need to go step for step on how the channels are working. and track the varaibles.


    Engineer the route (channel) how Data goes thru Map Worker to Backend for consume and storing.

End Goal:
    Have a functional MApReduce library to use on a wikipedia lookup or word count on a page certian or so


Notes from Russ:
    write to output channel for waiting. we have to many go routines. 
    open once like merge database. 



    maek a group channel layout 
    group channel is key- channel of pairs

    you it to group all key words like "cats" 
    testing purpose: 

    go run worker.go database.go 2>&1 | less


to find intermediade code-
	find / -name "map*.db" 2>/dev/null