package main

import (
	"flag"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"os"
	"tiny-dfs/gen-go/DataNode"
)

func main() {
	// 获取运行参数 {addr}
	addr := flag.String("addr", "localhost:9090", "Address to listen to")
	flag.Usage = Usage
	flag.Parse()

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryConf(nil)

	var transportFactory thrift.TTransportFactory
	if err := runServer(transportFactory, protocolFactory, *addr); err != nil {
		fmt.Println("error running server:", err)
	}
}

func runServer(transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory, addr string) error {
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		fmt.Println("run server error:", err)
	}
	fmt.Println(transport)

	handler := NewDataNodeHandler()
	processor := DataNode.NewDataNodeProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	return server.Serve()
}

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}
