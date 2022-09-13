package main

import (
	"flag"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"os"
	"strconv"
	"time"
	"tiny-dfs/gen-go/tdfs"
)

func main() {
	// 获取运行参数 {addr}
	port := flag.Int("port", 19101, "Address to listen to")
	limit := flag.Int("limit", 2, "The minimum number of replica")
	timeout := flag.Int64("interval", 30, "The interval between two heartbeats")
	flag.Usage = Usage
	flag.Parse()

	addr := "localhost:" + strconv.Itoa(*port)

	var protocolFactory thrift.TProtocolFactory
	//protocolFactory = thrift.NewTBinaryProtocolFactoryConf(nil)
	protocolFactory = thrift.NewTHeaderProtocolFactoryConf(nil)

	var transportFactory thrift.TTransportFactory
	//transportFactory = thrift.NewTBufferedTransportFactory(8192)
	//transportFactory = thrift.NewTTransportFactory()
	transportFactory = thrift.NewTHeaderTransportFactoryConf(nil, nil)

	serverTransport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		log.Fatalln("run server error:", err)
	}

	core := NewNameNodeCore(time.Duration(*timeout)*time.Second, *limit)
	handler := NewNameNodeHandler(core)
	processor := tdfs.NewNameNodeProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)

	log.Println("NameNode running on", addr)

	err = server.Serve()
	if err != nil {
		log.Fatalln("run server error:", err)
	}
}

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}
