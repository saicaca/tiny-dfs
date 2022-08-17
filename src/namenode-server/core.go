package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"tiny-dfs/gen-go/tdfs"
	dnc "tiny-dfs/src/datanode-client"
)

type NameNodeCore struct {
	MetaTrie      *PathTrie
	Registry      *Registry
	isSafeMode    bool // 是否处于安全模式
	exitSafeLimit int  // 退出安全模式所需的最小 DN 数量
}

func NewNameNodeCore(timeout time.Duration, safeLimit int) *NameNodeCore {
	core := &NameNodeCore{}
	registry := NewRegistry(timeout, func(addr string) {
		core.RemoveFromTrie(addr)
	})
	registry.registerAction = func() { // 有新 DN 注册时检查是否可以退出安全模式
		if core.isSafeMode && registry.dnCount >= core.exitSafeLimit {
			core.ExitSafeMode()
		}
	}
	registry.minReplica = safeLimit
	core.Registry = registry
	core.MetaTrie = NewPathTrie()
	core.isSafeMode = true
	core.exitSafeLimit = safeLimit
	return core
}

func (core *NameNodeCore) PutFile(metaMap map[string]*tdfs.Metadata, DNAddr string) {
	for path, meta := range metaMap {
		core.PutSingleFile(path, meta, DNAddr)
	}
}

func (core *NameNodeCore) PutSingleFile(path string, meta *tdfs.Metadata, DNAddr string) {
	res, err := core.MetaTrie.PutFile(path, DNAddr, meta)
	if err != nil {
		log.Println("Put file", path, "failed:", err)
	}

	// 若此次 PUT 创建或更新了文件，且已经退出安全模式，则删除所有旧的文件副本，并复制新的副本
	if res.Data["status"] == PUT_UPDATED && !core.isSafeMode {
		lst := res.Data["toDelete"].(map[string]struct{})
		for addr, _ := range lst {
			core.RemoveReplicaFromDataNode(addr, path)
		}
		// TODO 复制副本

	}
}

func (core *NameNodeCore) MakeReplica(path string) {
	// 获取 INode，获取文件当前所在的 DN
	DNSet := core.MetaTrie.GetFileNode(path).DNList
	// 获取可用的 DN 列表
	avaiNodes := core.Registry.GetSpareDataNodes()
	// 令 DN1 传输文件到 DN2
	sourceNode := ""
	for addr, _ := range DNSet {
		sourceNode = addr
		break
	}

	sourceClient, err := dnc.NewDataNodeClient(sourceNode)
	if err != nil {
		log.Panicln("创建副本时获取源节点 Client 失败：", err)
	}
	for _, target := range avaiNodes {
		if sourceNode == target {
			continue
		}
		log.Println("从", sourceNode, "复制文件", path, "到", target)
		_, err := sourceClient.MakeReplica(context.Background(), target, path)
		if err != nil {
			log.Println("从", sourceNode, "复制文件", path, "到", target, "失败", err)
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

func (core *NameNodeCore) RemoveReplicaFromDataNode(addr string, path string) {
	DNClient, err := dnc.NewDataNodeClient(addr)
	if err != nil {
		log.Panicln("删除副本时连接 DataNode", addr, "失败：", err)
	}
	_, err = DNClient.Delete(context.Background(), path)
	if err != nil {
		log.Panicln("删除 DataNode", addr, "文件副本", path, "失败：", err)
	}
}

// ExitSafeMode 在退出安全模式时执行的操作
func (core *NameNodeCore) ExitSafeMode() {
	log.Println("退出安全模式")
	// TODO 执行顺序有问题，应该先传完文件再执行这个
	core.MetaTrie.WalkAllFiles(func(path string, fileNode *INode) {
		if fileNode.Replica < 2 {
			fmt.Println(path, "需要复制")
			core.MakeReplica(path)
		}
	})
}
