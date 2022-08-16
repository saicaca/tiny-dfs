package main

import (
	"log"
	"time"
	"tiny-dfs/gen-go/tdfs"
)

type NameNodeCore struct {
	MetaTrie *PathTrie
	Registry *Registry
}

func NewNameNodeCore(timeout time.Duration) *NameNodeCore {
	core := &NameNodeCore{
		MetaTrie: NewPathTrie(),
		Registry: NewRegistry(timeout),
	}
	return core
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

}
