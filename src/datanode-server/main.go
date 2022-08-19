package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"log"
	"os"
	"tiny-dfs/gen-go/tdfs"
	"tiny-dfs/src/util"
)

func main() {
	// 获取运行参数 {addr}
	port := flag.String("port", "19200", "Port to listen to")
	root := flag.String("root", "./dn/", "Directories to store data")
	space := flag.String("space", "1GB", "Reserved space to store data")
	nnaddr := flag.String("nnaddr", "localhost:19100", "NameNode address")
	flag.Usage = Usage
	flag.Parse()

	addr := "localhost:" + *port

	spaceInByte := util.SizeToByte(*space)
	if spaceInByte < 0 {
		panic(errors.New("space 参数格式错误"))
	}

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
		NNAddr:     *nnaddr,
		isTest:     false,
		root:       *root,
		localIP:    transport.Addr().String(),
		totalSpace: spaceInByte,
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
