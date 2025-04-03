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
Get github.com/mattn/go-sqlite3
Go build 
Go init

Goal for Part 1:
    Get Map Task
    Map Handler, Reduce Handler
    MapTask: (should download select with SQL?)

    Engineer the route (channel) how Data goes thru Map Worker to Backend for consume and storing.

End Goal:
    Have a functional MApReduce library to use on a wikipedia lookup or word count on a page certian or so

