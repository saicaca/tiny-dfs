package dnc

import (
	"crypto/tls"
	"github.com/apache/thrift/lib/go/thrift"
	"tiny-dfs/gen-go/tdfs"
)

func RequestDataNode(addr string, f func(client *tdfs.DataNodeClient)) error {

	cfg := &thrift.TConfiguration{
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		ConnectTimeout: 0,
		SocketTimeout:  0,
		MaxMessageSize: 1024 * 1024 * 1024,
		MaxFrameSize:   1024 * 1024 * 1024,
	}

	var transportFactory thrift.TTransportFactory
	transportFactory = thrift.NewTHeaderTransportFactoryConf(nil, cfg)

	// 建立连接
	var transport thrift.TTransport
	transport = thrift.NewTSocketConf(addr, cfg)
	transport, err := transportFactory.GetTransport(transport)
	if err != nil {
		return err
	}
	if err := transport.Open(); err != nil {
		return err
	}
	defer transport.Close()
	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTHeaderProtocolFactoryConf(cfg)
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	client := tdfs.NewDataNodeClient(thrift.NewTStandardClient(iprot, oprot))
	f(client)
	return nil
}

func NewDataNodeClient(addr string) (*tdfs.DataNodeClient, error) {
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
	return tdfs.NewDataNodeClient(thrift.NewTStandardClient(iprot, oprot)), nil
}
