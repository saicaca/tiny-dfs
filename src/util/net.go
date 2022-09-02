package util

import (
	"fmt"
	"net"
)

func IsLocalHost(addr string) bool {
	if addr == "localhost" {
		return true
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	//for _, localAddr := range addrs {
	//	if ipnet, ok := localAddr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
	//		if ipnet.IP.To4() != nil {
	//			fmt.Println(ipnet.IP.String())
	//		}
	//	}
	//}

	for _, localAddr := range addrs {
		if ipnet, ok := localAddr.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil && ipnet.IP.String() == addr {
				return true
			}
		}
	}
	return false
}
