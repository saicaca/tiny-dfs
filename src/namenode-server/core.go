package main

import (
	"fmt"
	"log"
	"time"
	"tiny-dfs/gen-go/tdfs"
)

type NameNodeCore struct {
	MetaTrie *PathTrie
	Registry *Registry
}

func NewNameNodeCore(timeout time.Duration) *NameNodeCore {
	core := &NameNodeCore{}
	registry := NewRegistry(timeout, func(addr string) {
		core.RemoveFromTrie(addr)
	})
	core.Registry = registry
	core.MetaTrie = NewPathTrie()
	return core
}

func (core *NameNodeCore) RegisterDataNode() {

}

func (core *NameNodeCore) PutFile(metaMap map[string]*tdfs.Metadata, DNAddr string) {
	for path, meta := range metaMap {
		_, err := core.MetaTrie.PutFile(path, DNAddr, meta)
		if err != nil {
			log.Println("Put file", path, "failed:", err)
		} else {
			log.Println("成功添加文件", path)
		}
	}
}

func (core *NameNodeCore) RemoveFromTrie(DNAddr string) {
	dn, err := core.MetaTrie.RemoveByDN(DNAddr)
	if err != nil {
		log.Println("从文件树中移除 DataNode 文件发生错误：", err)
	}

	// TODO 补充文件副本
	fmt.Println(dn)
}
