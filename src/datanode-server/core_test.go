package main

import (
	"fmt"
	"testing"
	"tiny-dfs/gen-go/tdfs"
)

// 测试文件写入
func TestSave(t *testing.T) {
	core := &DataNodeCore{
		root: "./test-output/",
	}

	dataToSave := []byte("1145141919810")
	meta := &tdfs.Metadata{
		Name:      "testfile",
		Size:      114514,
		Mtime:     114514,
		IsDeleted: false,
	}

	if err := core.Save("testfile", dataToSave, meta); err != nil {
		t.Error("save failed:", err)
	}
}

// 测试元数据读取
func TestReadMeta(t *testing.T) {
	core := &DataNodeCore{
		root: "./test-output/",
	}
	mp, err := core.Scan()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(mp)
}
