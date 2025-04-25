package main

import (
	"fmt"
	"log"
	"net/rpc"
	"mapreducez"
)

func main() {
	master := mapreducez.Master{}
	rpc.DialHTTP("tcp", "localhost:1234")
}
