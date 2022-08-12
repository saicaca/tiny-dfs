package dnc

import (
	"crypto/tls"
	"github.com/apache/thrift/lib/go/thrift"
	"tiny-dfs/gen-go/DataNode"
)

func NewDataNodeClient(addr string) (*DataNode.DataNodeClient, error) {
	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryConf(nil)

	var transportFactory thrift.TTransportFactory

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
	defer transport.Close()
	if err := transport.Open(); err != nil {
		return nil, err
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	return DataNode.NewDataNodeClient(thrift.NewTStandardClient(iprot, oprot)), nil
}
