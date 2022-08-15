package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"tiny-dfs/gen-go/tdfs"
)

type DataNodeCore struct {
	root string
}

func NewDataNodeCore(root string) *DataNodeCore {
	return &DataNodeCore{
		root: root,
	}
}

type MetaMap map[string]tdfs.Metadata // map { 文件路径 -> 元数据 }

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
	return &tdfs.File{
		Data:     data,
		Medatada: meta,
	}, nil
}

// scan 扫描本地所存储的文件
func (core *DataNodeCore) Scan() (*MetaMap, error) {
	mp := make(MetaMap)
	err := filepath.Walk(core.root+META_PATH, func(path string, info fs.FileInfo, err error) error {
		// 如果读到的是目录，不做任何操作
		if info.IsDir() {
			return nil
		}

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
		mp[path] = m
		return nil
	})
	if err != nil {
		fmt.Println("read metadata failed:", err)
		return nil, err
	}
	return &mp, nil
}
