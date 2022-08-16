package main

import (
	"context"
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
	stat   *tdfs.DNStat
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

// StartHeartBeat 启动心跳检测
func (r *Registry) StartHeartBeat() {
	log.Println("心跳检测启动")

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for range ticker.C { // 进行一次循环
			for _, item := range r.dnmap { // 遍历节点
				client, err := dnc.NewDataNodeClient(item.Addr)
				if err != nil { // 连接 DataNode 失败
					log.Println("DataNode", item.Addr, "无法连接")
					if item.start.Add(time.Second * 20).Before(time.Now()) {
						log.Println("DataNode", item.Addr, "长时间无法连接，移除节点")
						delete(r.dnmap, item.Addr)
						r.deleteAction(item.Addr)
					}
				} else { // 连接 DataNode 成功
					log.Println("DataNode", item.Addr, "连接正常")
					item.start = time.Now()
					stat, err := client.Ping(context.Background())
					if err != nil {
						log.Println("获取 DataNode 状态信息失败")
					} else {
						item.stat = stat // 保存 DataNode 状态数据
						log.Println(stat)
					}
				}
			}
		}
	}()

}

type byRemain []*DNItem

func (s byRemain) Len() int {
	return len(s)
}
func (s byRemain) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byRemain) Less(i, j int) bool { // 按剩余空间大小排序
	remain1 := s[i].stat.TotalSpace - s[i].stat.UsedSpace
	remain2 := s[j].stat.TotalSpace - s[j].stat.UsedSpace
	return remain1 > remain2 // 从大到小排序
}
func (r *Registry) GetSpareDataNodes() []string {
	lst := make(byRemain, len(r.dnmap))
	i := 0
	for key := range r.dnmap {
		lst[i] = r.dnmap[key]
		i++
	}
	sort.Sort(byRemain(lst))

	ipList := make([]string, 3)
	for i := 0; i < 3; i++ {
		ipList[i] = lst[i].Addr
	}
	return ipList
}
