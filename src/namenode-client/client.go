package nnc

import (
	"crypto/tls"
	"github.com/apache/thrift/lib/go/thrift"
	"tiny-dfs/gen-go/tdfs"
)

func RequestNameNode(addr string, f func(client *tdfs.NameNodeClient) error) {
	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTHeaderProtocolFactoryConf(nil)

	var transportFactory thrift.TTransportFactory
	transportFactory = thrift.NewTHeaderTransportFactoryConf(nil, nil)

	cfg := &thrift.TConfiguration{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	// 建立连接
	var transport thrift.TTransport
	transport = thrift.NewTSocketConf(addr, cfg)
	transport, err := transportFactory.GetTransport(transport)
	defer transport.Close()
	if err != nil {
		panic(err)
	}
	if err := transport.Open(); err != nil {
		panic(err)
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	client, err := tdfs.NewNameNodeClient(thrift.NewTStandardClient(iprot, oprot)), nil
	err = f(client)
	if err != nil {
		panic(err)
	}
}

func NewNameNodeClient(addr string) (*tdfs.NameNodeClient, error) {
	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTHeaderProtocolFactoryConf(nil)

	var transportFactory thrift.TTransportFactory
	transportFactory = thrift.NewTHeaderTransportFactoryConf(nil, nil)

	cfg := &thrift.TConfiguration{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	// 建立连接
	var transport thrift.TTransport
	transport = thrift.NewTSocketConf(addr, cfg)
	transport, err := transportFactory.GetTransport(transport)
	if err != nil {
		return nil, err
	}
	if err := transport.Open(); err != nil {
		return nil, err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	return tdfs.NewNameNodeClient(thrift.NewTStandardClient(iprot, oprot)), nil
}
