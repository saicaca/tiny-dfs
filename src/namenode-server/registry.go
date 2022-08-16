package main

import (
	"log"
	"sort"
	"sync"
	"time"
	"tiny-dfs/gen-go/tdfs"
	dnc "tiny-dfs/src/datanode-client"
)

type DNItem struct {
	Addr   string
	start  time.Time
	client *tdfs.DataNodeClient
}

type Registry struct {
	timeout      time.Duration
	mu           sync.Mutex
	dnmap        map[string]*DNItem
	deleteAction func(addr string)
}

const (
	defaultTimeout = time.Minute * 5
)

func NewRegistry(timeout time.Duration, deleteAction func(addr string)) *Registry {
	registry := &Registry{
		dnmap:        make(map[string]*DNItem),
		timeout:      timeout,
		deleteAction: deleteAction,
	}
	registry.StartHeartBeat() // 开始心跳检测
	return registry
}

func (r *Registry) PutDataNode(addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.dnmap[addr]
	if s == nil { // DN 未注册
		// TODO 暂时停用DN客户端创建
		//client, err := dnc.NewDataNodeClient(addr) // 获取 DN Client
		//if err != nil {
		//	log.Panicln("Failed to create DataNodeClient", err)
		//	return err
		//}
		r.dnmap[addr] = &DNItem{
			Addr:   addr,
			start:  time.Now(),
			client: nil,
		}
	} else {
		s.start = time.Now()
	}
	return nil
}

func (r *Registry) AliveDataNodes(deleteAction func(addr string)) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.dnmap {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.dnmap, addr)
			r.deleteAction(addr)
		}
	}
	sort.Strings(alive)
	return alive
}

func (r *Registry) StartHeartBeat() {
	log.Println("心跳检测启动")

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for range ticker.C { // 进行一次循环
			for _, item := range r.dnmap { // 遍历节点
				_, err := dnc.NewDataNodeClient(item.Addr)
				if err != nil {
					log.Println("DataNode", item.Addr, "无法连接")
					if item.start.Add(time.Second * 20).Before(time.Now()) {
						log.Println("DataNode", item.Addr, "长时间无法连接，移除节点")
						delete(r.dnmap, item.Addr)
						r.deleteAction(item.Addr)
					}
				} else {
					log.Println("DataNode", item.Addr, "连接正常")
					item.start = time.Now()
				}
			}
		}
	}()

}
