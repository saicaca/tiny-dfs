package main

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"tiny-dfs/gen-go/tdfs"
)

// PathTrie 是 NameNode 中管理文件系统目录的数据结构
type PathTrie struct {
	root *INode
}

// NewPathTrie 创建新的 PathTrie
func NewPathTrie() *PathTrie {
	return &PathTrie{
		root: NewDirNode(),
	}
}

// INode 是 PathTrie 中的节点
type INode struct {
	IsDir    bool              // true 表示本节点为目录，false 则为文件
	Children map[string]*INode // 本目录下的文件和子目录 INode 列表
	Meta     tdfs.Metadata     // 文件元数据
	Replica  uint32            // 本文件当前副本数
	DNList   set               // 存有本文件的 DataNode 的 UUID 集合
}

type (
	set map[string]struct{}
)

func NewDirNode() *INode {
	return &INode{
		IsDir:    true,
		Children: make(map[string]*INode),
	}
}

// 获取指定目录的 INode，create 表示当指定目录不存在时是否自动创建
func (t *PathTrie) getDir(path string, create bool) (*INode, error) {
	currNode := t.root
	dirs := splitPath(path)
	for _, dir := range dirs {
		next, ok := currNode.Children[dir]
		if ok && next.IsDir { // 存在子文件夹
			currNode = next
		} else if ok && !next.IsDir { // 子 INode 存在但为文件，返回错误
			return nil, errors.New("目录创建失败：存在与所找目录同名的文件：" + dir)
		} else {
			if create { // 不存在目录，自动创建
				newNode := NewDirNode()
				currNode.Children[dir] = newNode
				currNode = newNode
			} else { // 不存在目录，返回错误
				return nil, errors.New("目录查找失败，不存在此目录：" + dir)
			}
		}
	}
	return currNode, nil
}

func (t *PathTrie) MkdirAll(path string) error {
	if _, err := t.getDir(path, true); err != nil {
		return err
	}
	return nil
}

func (t *PathTrie) FindDir(path string) (*INode, error) {
	node, err := t.getDir(path, false)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// 保存文件
func (t *PathTrie) PutFile(path string, meta *tdfs.Metadata) error {
	dir, fileName := filepath.Split(path)
	dirNode, err := t.getDir(dir, true)
	if err != nil {
		return err
	}
	dirNode.Children[fileName] = &INode{
		IsDir:   false,
		Meta:    *meta,
		Replica: 1,
		DNList:  make(set),
	}
	log.Println("put file:", path)
	return nil
}

// 显示指定目录下的所有文件和子目录
func (t *PathTrie) List(path string) {
	path = beautifyPath(path)

	target, err := t.FindDir(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	for name, node := range target.Children {
		if node.IsDir {
			fmt.Printf("%s\t\tDIR\n", path+name)
		} else {
			fmt.Printf("%s\t\tFILE\n", path+name)
		}
	}
}

// 拆分路径
func splitPath(path string) []string {
	splitFn := func(c rune) bool {
		return c == '\\' || c == '/'
	}
	return strings.FieldsFunc(path, splitFn)
}

// 统一路径字符串格式
func beautifyPath(path string) string {
	parts := splitPath(path)
	if len(parts) == 0 {
		return "\\"
	}
	return "\\" + strings.Join(parts, "\\") + "\\"
}
