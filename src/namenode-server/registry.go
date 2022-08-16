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
	timeout time.Duration
	mu      sync.Mutex
	dnmap   map[string]*DNItem
}

const (
	defaultTimeout = time.Minute * 5
)

func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{
		dnmap:   make(map[string]*DNItem),
		timeout: timeout,
	}
}

var DefaultRegister = NewRegistry(defaultTimeout)

func (r *Registry) PutDataNode(addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.dnmap[addr]
	if s == nil { // DN 未注册
		client, err := dnc.NewDataNodeClient(addr) // 获取 DN Client
		if err != nil {
			log.Panicln("Failed to create DataNodeClient", err)
			return err
		}
		r.dnmap[addr] = &DNItem{
			Addr:   addr,
			start:  time.Now(),
			client: client,
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
			deleteAction(addr)
		}
	}
	sort.Strings(alive)
	return alive
}
