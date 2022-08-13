package main

import (
	"encoding/gob"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

type DataNodeCore struct {
	root string
}

type MetaData struct {
	IsDeleted bool
	Name      string
	Mtime     int64
	Size      int64
}

type MetaMap map[string]MetaData // map { 文件路径 -> 元数据 }

const (
	META_PATH      = "meta/"
	DATA_PATH      = "data/"
	META_EXTENSION = ".meta"
)

func (core *DataNodeCore) save(path string, data []byte, meta MetaData) error {
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
	if err != nil {
		fmt.Println("create metadata failed:", err)
		return err
	}
	enc := gob.NewEncoder(metaFile)
	err = enc.Encode(meta)
	if err != nil {
		fmt.Println("write metadata failed:", err)
		return err
	}

	return nil
}

// scan 扫描本地所存储的文件
func (core *DataNodeCore) scan() (*MetaMap, error) {
	mp := make(MetaMap)
	err := filepath.Walk(core.root+META_PATH, func(path string, info fs.FileInfo, err error) error {
		// 如果读到的是目录，不做任何操作
		if info.IsDir() {
			return nil
		}

		var m MetaData
		file, err := os.Open(path)
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
