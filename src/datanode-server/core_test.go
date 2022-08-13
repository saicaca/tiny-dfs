package main

import (
	"fmt"
	"testing"
)

// 测试文件写入
func TestSave(t *testing.T) {
	core := &DataNodeCore{
		root: "./test-output/",
	}

	dataToSave := []byte("1145141919810")
	meta := MetaData{
		Name:      "testfile",
		Size:      114514,
		Mtime:     114514,
		IsDeleted: false,
	}

	if err := core.save("testfile", dataToSave, meta); err != nil {
		t.Error("save failed:", err)
	}
}

// 测试元数据读取
func TestReadMeta(t *testing.T) {
	core := &DataNodeCore{
		root: "./test-output/",
	}
	mp, err := core.scan()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(mp)
}
