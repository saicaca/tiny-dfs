package main

import (
	"fmt"
	"testing"
	"time"
)

// 定时器测试
func TestTicket(t *testing.T) {
	ticker := time.NewTicker(time.Second)
	go func() {
		//for t := range ticker.C {
		//	fmt.Println("tick at", t)
		//}
		for range ticker.C {
			fmt.Println("tick at")
		}
	}()

	time.Sleep(time.Second * 30)
	ticker.Stop()
	fmt.Println("ticker stop")
}
