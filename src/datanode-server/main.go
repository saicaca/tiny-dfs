package main

import (
	"flag"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"os"
	"tiny-dfs/gen-go/tdfs"
)

func main() {
	// 获取运行参数 {addr}
	port := flag.String("port", "19200", "Port to listen to")
	root := flag.String("root", "./playground/dn1/", "Directories to store data")
	flag.Usage = Usage
	flag.Parse()

	addr := "localhost:" + *port
	nnaddr := "localhost:19100"

	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryConf(nil)

	var transportFactory thrift.TTransportFactory
	transportFactory = thrift.NewTBufferedTransportFactory(8192)

	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		fmt.Println("run server error:", err)
	}

	// DataNode 启动配置
	config := &DNConfig{
		NNAddr:     nnaddr,
		isTest:     false,
		root:       *root,
		localIP:    transport.Addr().String(),
		totalSpace: 10 * 1024 * 1024 * 1024,
	}

	fmt.Println("启动配置", *config)

	core, err := NewDataNodeCore(config)
	if err != nil {
		log.Println("Failed to create DataNodeCore:", err)
	}

	handler := NewDataNodeHandler(core)
	processor := tdfs.NewDataNodeProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	log.Println("DataNode server running on", addr)

	if err := server.Serve(); err != nil {
		log.Fatalln("Failed to start DataNode server:", err)
	}
}

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}
