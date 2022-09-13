package main

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"tiny-dfs/gen-go/tdfs"
	dnc "tiny-dfs/src/datanode-client"
)

const (
	METADATA_DIR      = "./meta/"
	METADATA_PREFIX   = "meta"
	RETAINED_METADATA = 3
)

type NameNodeCore struct {
	MetaTrie      *PathTrie
	Registry      *Registry
	putMap        map[string]*PutTask
	isSafeMode    bool // 是否处于安全模式
	exitSafeLimit int  // 退出安全模式所需的最小 DN 数量
	chunkMap      map[string]mapset.Set[string]
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
	registry.deleteAction = func(addr string) {
		core.RemoveFromTrie(addr)
	}
	registry.minReplica = safeLimit
	core.Registry = registry
	core.isSafeMode = true
	core.exitSafeLimit = safeLimit
	core.putMap = make(map[string]*PutTask)
	core.chunkMap = make(map[string]mapset.Set[string])

	if err := os.MkdirAll(METADATA_DIR, os.ModePerm); err != nil {
		log.Fatalln("Failed to create metadata directory：", METADATA_DIR)
	}

	core.LoadMetadata()

	return core
}

func (core *NameNodeCore) PutFileLegacy(metaMap map[string]*tdfs.Metadata, DNAddr string) {
	for path, meta := range metaMap {
		core.PutSingleFile(path, meta, DNAddr)
	}
}

func (core *NameNodeCore) PutSingleFile(path string, meta *tdfs.Metadata, DNAddr string) {
	res, err := core.MetaTrie.PutFileLegacy(path, DNAddr, meta)
	if err != nil {
		log.Println("Put file", path, "failed:", err)
	}

	// 若此次 PUT 创建或更新了文件，且已经退出安全模式，则删除所有旧的文件副本，并复制新的副本
	if res.Data["status"] == PUT_UPDATED && !core.isSafeMode {
		lst := res.Data["toDelete"].(CSet)
		for addr, _ := range lst {
			core.RemoveReplicaFromDataNode(addr, path)
		}
		core.MakeReplica(path)
	}
}

func (core *NameNodeCore) SetDeleted(path string) error {
	node := core.MetaTrie.GetFileNode(path)
	meta := node.Meta
	meta.Mtime = time.Now().UnixMilli()
	meta.IsDeleted = true
	err := core.UpdateMetadata(path, &meta)
	return err
}

func (core *NameNodeCore) Move(originPath string, newPath string) error {
	newDir, newFileName := filepath.Split(newPath)

	// 获取被移动文件信息
	oldNode := core.MetaTrie.GetFileNode(originPath)
	if oldNode == nil {
		return errors.New("文件 " + originPath + " 不存在")
	}

	// 获取新的所在目录节点
	dir, err := core.MetaTrie.getDir(newDir, true)
	if err != nil {
		return err
	}

	// 检查是否已存在同名文件
	nodeToWrite := core.MetaTrie.GetFileNode(newPath)
	if core.MetaTrie.GetFileNode(newPath) != nil && !nodeToWrite.Meta.IsDeleted {
		return errors.New("路径非法：文件 " + newPath + " 已存在")
	}

	now := time.Now().UnixMilli()

	newMetadata := oldNode.Meta
	newMetadata.Mtime = now
	newMetadata.Name = newFileName
	newMetadata.IsDeleted = false
	dir.Children[newFileName] = &INode{
		IsDir:    false,
		Replica:  0,
		DNList:   make(CSet),
		Meta:     newMetadata,
		Children: make(map[string]*INode),
	}

	// 修改原节点元数据
	oldNode.Meta.Mtime = now
	oldNode.Meta.Size = 0
	oldNode.Meta.IsDeleted = true

	// 令 DataNode 移动文件
	for addr, _ := range oldNode.DNList {
		dnclient, err := dnc.NewDataNodeClient(addr)
		if err != nil {
			return err
		}
		err = dnclient.MoveFile(context.Background(), originPath, newPath, now)
		if err != nil {
			return err
		}
	}
	return nil
}

func (core *NameNodeCore) UpdateMetadata(path string, metadata *tdfs.Metadata) error {
	node := core.MetaTrie.GetFileNode(path)
	node.Meta = *metadata

	successList := make(CSet)
	for addr, _ := range node.DNList {
		client, err := dnc.NewDataNodeClient(addr)
		if err != nil {
			log.Println("更新文件元数据时创建 DataNode 客户端", addr, "失败：", err)
			continue
		}
		err = client.UpdateMetadata(context.Background(), path, metadata)
		if err != nil {
			return err
		}
		successList[addr] = void
	}
	node.DNList = successList // 更新成功的节点
	return nil
}

func (core *NameNodeCore) MakeReplica(path string) {
	// 获取 INode，获取文件当前所在的 DN
	DNSet := core.MetaTrie.GetFileNode(path).DNList
	//log.Println("被复制文件所在DN：", DNSet)
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
		log.Println("从", sourceNode, "为文件", path, "创建副本到", target)
		_, err := sourceClient.MakeReplica(context.Background(), target, path)
		if err != nil {
			log.Println("从", sourceNode, "复制文件", path, "到", target, "失败", err)
		}
	}
}

func (core *NameNodeCore) RemoveFromTrie(DNAddr string) {
	res, err := core.MetaTrie.RemoveByDN(DNAddr)
	if err != nil {
		log.Println("从文件树中移除 DataNode 文件发生错误：", err)
	}
	// 补充文件副本
	files := (res.Data["underLimit"]).([]string)
	for _, path := range files {
		core.MakeReplica(path)
	}
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
	core.isSafeMode = false
	// TODO 执行顺序有问题，应该先传完文件再执行这个
	core.MetaTrie.WalkAllFiles(func(path string, fileNode *INode) {
		if fileNode.Replica < 2 {
			fmt.Println(path, "需要复制")
			core.MakeReplica(path)
		}
	})
}

type PutTask struct {
	path     string
	total    int64
	finished int64
	chunkIds []string
	meta     *tdfs.Metadata
	mu       sync.Mutex
}

func (core *NameNodeCore) InitializePut(path string, meta *tdfs.Metadata, totalChunk int64) (string, error) {
	task := &PutTask{}
	taskId := uuid.NewString()
	task.path = path
	task.total = totalChunk
	task.finished = 0
	task.chunkIds = make([]string, totalChunk)
	task.meta = meta

	core.putMap[taskId] = task

	return taskId, nil
}

func (core *NameNodeCore) PutChunk(taskId string, seq int64, chunkId string) (*tdfs.PutChunkResp, error) {
	task := core.putMap[taskId]
	if task.chunkIds[seq] == "" {
		task.mu.Lock()
		defer task.mu.Unlock()
		task.chunkIds[seq] = chunkId
		task.finished++
		if task.finished == task.total {
			_ = core.MetaTrie.PutFile(task.path, task.meta, task.chunkIds)
			log.Println("Put file", task.path, "with", task.total, "chunks")
			core.PersistMetadata()
			return &tdfs.PutChunkResp{IsFinished: true}, nil
		} else {
			return &tdfs.PutChunkResp{IsFinished: false}, nil
		}
	} else if task.chunkIds[seq] == chunkId { // chunk already exists
		return &tdfs.PutChunkResp{IsFinished: task.finished == task.total}, nil
	} else { // different chunks with same offset
		return nil, errors.New("different chunks with same offset " + strconv.FormatInt(seq, 10))
	}
}

func (core *NameNodeCore) GetChunks(path string, offset int64, size int64) (*tdfs.ChunkList, error) {
	node := core.MetaTrie.GetFileNode(path)
	if node == nil {
		return nil, errors.New("File " + path + " not found")
	}
	lst := node.Chunks[offset : offset+size]
	ret := &tdfs.ChunkList{
		Offset: offset,
		Chunks: make([]*tdfs.ChunkInfo, size),
	}
	for _, v := range lst {
		info := &tdfs.ChunkInfo{
			ChunkId: v,
		}
		// TODO get datanode list
		ret.Chunks = append(ret.Chunks, info)
	}

	return ret, nil
}

func (core *NameNodeCore) ReceiveChunks(chunks []string, datanodeIP string) {
	for _, chunk := range chunks {
		if _, ok := core.chunkMap[chunk]; !ok {
			core.chunkMap[chunk] = mapset.NewSet[string]()
		}
		core.chunkMap[chunk].Add(datanodeIP)
		log.Println("Loaded chunk", chunk, "on datanode", datanodeIP)
	}
}

func (core *NameNodeCore) PersistMetadata() {
	fileName := METADATA_PREFIX + strconv.FormatInt(time.Now().UnixMicro(), 10)
	metaFile, err := os.Create(METADATA_DIR + fileName)
	defer metaFile.Close()
	if err != nil {
		log.Panicln("Failed to persist metadata:", err)
	}
	enc := gob.NewEncoder(metaFile)
	err = enc.Encode(*core.MetaTrie)
	if err != nil {
		log.Panicln("Failed to persist metadata:", err)
	}
	log.Println("Metadata", fileName, "persisted")
	// TODO remove earlier metadata files
}

func (core *NameNodeCore) LoadMetadata() {
	// list all metadata files
	var fileList []string
	err := filepath.Walk(METADATA_DIR, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		fileName := info.Name()
		if !strings.HasPrefix(fileName, METADATA_PREFIX) {
			return nil
		}
		fileList = append(fileList, fileName)
		return nil
	})
	if err != nil {
		log.Panicln("Failed to load metadata files from disk", err)
	}

	// sort the files and use the latest one
	sort.Sort(sort.Reverse(sort.StringSlice(fileList)))

	core.MetaTrie = NewPathTrie()
	if len(fileList) == 0 {
		log.Println("Did not find metadata to load")
		return
	}

	log.Println("Loading metadata", fileList[0])
	file, err := os.Open(METADATA_DIR + fileList[0])
	defer file.Close()
	if err != nil {
		log.Panicln("Failed to open metadata file", fileList[0], err)
	}
	dec := gob.NewDecoder(file)
	err = dec.Decode(core.MetaTrie)
	if err != nil {
		log.Panicln("Failed to decode metadata", fileList[0], err)
	}
	log.Println("Metadata", fileList[0], "loaded")

	// delete earlier metadata files
	for i := RETAINED_METADATA; i < len(fileList); i++ {
		idx := i
		go func() {
			err := os.Remove(METADATA_DIR + fileList[idx])
			if err != nil {
				log.Println("Failed to delete outdated metadata file", fileList[idx], err)
			}
		}()
	}
}
