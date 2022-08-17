package main

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"tiny-dfs/gen-go/tdfs"
	dnc "tiny-dfs/src/datanode-client"
	nnc "tiny-dfs/src/namenode-client"
)

type DataNodeCore struct {
	root       string
	nnclient   *tdfs.NameNodeClient
	localIP    string
	fileNum    int64
	totalSpace int64
	usedSpace  int64
	traffic    int64
}

type DNConfig struct {
	root       string // 文件存储根目录
	NNAddr     string // 连接的 NameNode 地址
	isTest     bool   // 是否为单机测试模式
	localIP    string
	totalSpace int64
}

var defaultCtx = context.Background()

func NewDataNodeCore(config *DNConfig) (*DataNodeCore, error) {
	core := &DataNodeCore{}
	core.root = config.root
	core.localIP = config.localIP
	core.totalSpace = config.totalSpace
	core.fileNum = 0
	core.usedSpace = 0
	core.traffic = 0

	// 创建存储目录
	if err := os.MkdirAll(core.root+META_PATH, os.ModePerm); err != nil {
		log.Fatalln("创建存储目录失败：", core.root+META_PATH)
	}
	if err := os.MkdirAll(core.root+DATA_PATH, os.ModePerm); err != nil {
		log.Fatalln("创建存储目录失败：", core.root+DATA_PATH)
	}

	// 非本地测试模式，扫描文件并发送至 NameNode
	if !config.isTest {
		nnclient, err := nnc.NewNameNodeClient(config.NNAddr)
		if err != nil {
			log.Println("Failed to create NameNode client:", err)
			return nil, err
		}
		core.nnclient = nnclient
		if err := core.Register(); err != nil {
			log.Println("Failed to register:", err)
			return nil, err
		}
	}
	return core, nil
}

type MetaMap map[string]*tdfs.Metadata // map { 文件路径 -> 元数据 }

const (
	META_PATH      = "meta/"
	DATA_PATH      = "data/"
	META_EXTENSION = ".meta"
)

func (core *DataNodeCore) Save(path string, data []byte, meta *tdfs.Metadata) error {
	// 如果保存路径不存在则创建路径
	dataPath := core.root + DATA_PATH + path
	metaPath := core.root + META_PATH + path + META_EXTENSION
	if err := os.MkdirAll(filepath.Dir(dataPath), 0750); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(metaPath), 0750); err != nil {
		return err
	}

	// 保存文件
	err := os.WriteFile(dataPath, data, 0777)
	if err != nil {
		fmt.Println("write file error:", err)
		return err
	}

	// 保存元文件
	metaFile, err := os.Create(metaPath)
	defer metaFile.Close()
	if err != nil {
		fmt.Println("create metadata failed:", err)
		return err
	}
	enc := gob.NewEncoder(metaFile)
	err = enc.Encode(*meta)
	if err != nil {
		fmt.Println("write metadata failed:", err)
		return err
	}

	// 更新统计数据
	core.fileNum++
	core.traffic += int64(len(data))

	return nil
}

func (core *DataNodeCore) Get(path string) (*tdfs.File, error) {
	dataPath := core.root + DATA_PATH + path
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, errors.New("Failed to load file:" + dataPath)
	}

	var meta *tdfs.Metadata
	metaPath := core.root + META_PATH + path + META_EXTENSION
	metaFile, err := os.Open(metaPath)
	if err != nil {
		return nil, errors.New("Failed to load metadata:" + metaPath)
	}
	defer metaFile.Close()
	dec := gob.NewDecoder(metaFile)
	err = dec.Decode(&meta)
	if err != nil {
		return nil, errors.New("Failed to decode metadata:" + metaPath)
	}

	// 更新统计数据
	core.traffic += int64(len(data))

	return &tdfs.File{
		Data:     data,
		Medatada: meta,
	}, nil
}

func (core *DataNodeCore) MakeReplica(target string, path string) {
	if target == core.localIP {
		return
	}

	file, err := core.Get(path)
	if err != nil {
		log.Panicln("创建副本时读取文件失败：", err)
	}

	receiveClient, err := dnc.NewDataNodeClient(target)
	if err != nil {
		log.Panicln("创建副本时连接目标 DataNode 失败：", err)
	}
	_, err = receiveClient.ReceiveReplica(context.Background(), path, file)
	if err != nil {
		log.Panicln("创建副本时复制文件失败：", err)
	}
}

func (core *DataNodeCore) Delete(path string) {
	dataPath := core.root + DATA_PATH + path
	metaPath := core.root + META_PATH + path + META_EXTENSION
	if err := os.Remove(dataPath); err != nil {
		log.Println("删除文件", path, "失败：", err)
	}
	if err := os.Remove(metaPath); err != nil {
		log.Println("删除元数据", path, "失败：", err)
	}
}

// scan 扫描本地所存储的文件
func (core *DataNodeCore) Scan() (*MetaMap, error) {
	core.fileNum = 0

	mp := make(MetaMap)

	fmt.Println(core.root + META_PATH)

	err := filepath.Walk(core.root+META_PATH, func(path string, info fs.FileInfo, err error) error {

		// 如果读到的是目录，不做任何操作
		if info.IsDir() {
			return nil
		}

		// 读取 metadata
		var m tdfs.Metadata
		file, err := os.Open(path)
		defer file.Close()
		if err != nil {
			fmt.Println("open path failed:", err)
			return err
		}
		dec := gob.NewDecoder(file)
		err = dec.Decode(&m)
		if err != nil {
			fmt.Println("decode metadata failed:", err)
			return err
		}

		remotePath := path[len(filepath.Clean(core.root+META_PATH)) : len(path)-len(META_EXTENSION)] // 去除本地路径前缀，去除元文件扩展名
		mp[remotePath] = &m

		// 更新统计数据
		core.fileNum++
		core.usedSpace += m.Size

		return nil
	})
	if err != nil {
		fmt.Println("read metadata failed:", err)
		return nil, err
	}
	return &mp, nil
}

// 向 NameNode 注册服务
func (core *DataNodeCore) Register() error {
	metaMap, err := core.Scan()
	if err != nil {
		log.Panicln("Failed to scan files:", err)
		return err
	}
	resp, err := core.nnclient.Register(defaultCtx, *metaMap, core.localIP)
	if err != nil {
		return err
	}
	log.Println(resp)
	return nil
}

// 获取统计数据
func (core *DataNodeCore) GetStat() *tdfs.DNStat {
	stat := &tdfs.DNStat{
		FileNum:    core.fileNum,
		UsedSpace:  core.usedSpace,
		TotalSpace: core.totalSpace,
		Traffic:    core.traffic,
	}
	return stat
}
