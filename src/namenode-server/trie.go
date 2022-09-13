package main

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"tiny-dfs/gen-go/tdfs"
	"tiny-dfs/src/shared"
)

// PathTrie 是 NameNode 中管理文件系统目录的数据结构
type PathTrie struct {
	Root       *INode
	FilesByDN  map[string][]string
	MinReplica int32
}

// NewPathTrie 创建新的 PathTrie
func NewPathTrie() *PathTrie {
	return &PathTrie{
		Root:       NewDirNode(),
		FilesByDN:  make(map[string][]string),
		MinReplica: 3,
	}
}

// INode 是 PathTrie 中的节点
type INode struct {
	IsDir    bool              // true 表示本节点为目录，false 则为文件
	Children map[string]*INode // 本目录下的文件和子目录 INode 列表
	Meta     tdfs.Metadata     // 文件元数据
	Replica  int32             // 本文件当前副本数
	DNList   CSet              // 存有本文件的 DataNode 的 IP 集合
	Chunks   []string          // List of chunks
}

type (
	CSet map[string]struct{}
)

var void struct{}

func NewDirNode() *INode {
	return &INode{
		IsDir:    true,
		Children: make(map[string]*INode),
	}
}

// 获取指定目录的 INode，create 表示当指定目录不存在时是否自动创建
func (t *PathTrie) getDir(path string, create bool) (*INode, error) {
	currNode := t.Root
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

type PutResult int32

const (
	PUT_SUCCESS  = 0
	PUT_OUTDATED = 1
	PUT_UPDATED  = 2
)

// 保存文件
func (t *PathTrie) PutFileLegacy(path string, DNAddr string, meta *tdfs.Metadata) (*shared.Result, error) {
	// 获取文件所在目录节点
	dir, fileName := filepath.Split(path)
	dirNode, err := t.getDir(dir, true)
	if err != nil {
		return nil, err
	}

	result := shared.NewResult()

	// 获取旧的修改时间
	var currModTime int64 = 0
	if dirNode.Children[fileName] != nil {
		currModTime = dirNode.Children[fileName].Meta.Mtime
	}

	newModTime := meta.Mtime
	if newModTime > currModTime { // PUT 的文件比现有版本新或为新建文件，更新现存的所有副本
		result.Data["status"] = PUT_UPDATED
		if dirNode.Children[fileName] != nil { // 文件存在，将当前所有副本所在节点放入待删除
			toDeleteSet := dirNode.Children[fileName].DNList
			delete(toDeleteSet, DNAddr)
			result.Data["toDelete"] = toDeleteSet
		} else { // 新建文件，待更新列表为空列表
			result.Data["toDelete"] = make(CSet)
		}

		// 新建 INode
		dirNode.Children[fileName] = &INode{
			IsDir:   false,
			Meta:    *meta,
			Replica: 1,
			DNList:  make(CSet),
		}
		dirNode.Children[fileName].DNList[DNAddr] = void // 将当前 DN 地址加入到副本地址列表
		log.Println("新建或更新文件", path, "，来自 DataNode 节点", DNAddr)
	} else if newModTime == currModTime { // PUT 的文件和当前最新版本相同，副本数 + 1
		result.Data["status"] = PUT_SUCCESS
		dirNode.Children[fileName].Replica += 1
		dirNode.Children[fileName].DNList[DNAddr] = void
		if !meta.IsDeleted {
			log.Println("新增文件副本", path, "，来自 DataNode 节点", DNAddr)
		}
	} else { // 当前 PUT 的文件版本低于最新版本
		result.Data["status"] = PUT_OUTDATED
		result.Data["source"] = dirNode.Children[fileName].DNList
		if meta.IsDeleted {
			log.Println("来自 DataNode 节点", DNAddr, "的文件", path, "版本低于现有，将自动更新")
		}
	}

	// 写入 DN地址 -> 文件 map
	t.FilesByDN[DNAddr] = append(t.FilesByDN[DNAddr], path)

	return result, nil
}

// PutFile
func (t *PathTrie) PutFile(path string, meta *tdfs.Metadata, chunks []string) error {
	dir, fileName := filepath.Split(path)
	dirNode, err := t.getDir(dir, true)
	if err != nil {
		return err
	}

	dirNode.Children[fileName] = &INode{
		IsDir:   false,
		Meta:    *meta,
		Chunks:  chunks,
		Replica: 1, // Temporary, for compatibility with older versions
	}
	return nil
}

func (t *PathTrie) GetFileNode(path string) *INode {
	dir, fileName := filepath.Split(path)
	dirNode, err := t.getDir(dir, true)
	if err != nil {
		log.Println("获取文件", path, "失败：", err)
		return nil
	}
	return dirNode.Children[fileName]
}

// RemoveByDN 从 trie 中移除指定 DN 下所有文件信息（用于 DN 断连的情况），返回副本数量低于最低值的文件列表
func (t *PathTrie) RemoveByDN(DNAddr string) (*shared.Result, error) {
	fileList := t.FilesByDN[DNAddr]

	log.Println(DNAddr, "存储的文件副本：", fileList)

	var filesToCopy []string

	for _, filePath := range fileList {
		dir, fileName := filepath.Split(filePath)
		dirNode, err := t.getDir(dir, true)
		if err != nil {
			return nil, err
		}

		dirNode.Children[fileName].Replica -= 1           // 减少副本数量
		delete(dirNode.Children[fileName].DNList, DNAddr) // 从文件的副本 DataNode 列表中移除当前 DataNode
		if dirNode.Children[fileName].Replica < t.MinReplica {
			filesToCopy = append(filesToCopy, filePath)
		}
	}

	result := &shared.Result{
		Data: map[string]interface{}{},
	}
	result.Data["underLimit"] = filesToCopy

	return result, nil
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

// 返回指定目录下的所有文件和子目录的信息
func (t *PathTrie) ListStat(path string) (map[string]*tdfs.FileStat, error) {
	path = beautifyPath(path)

	target, err := t.FindDir(path)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	res := make(map[string]*tdfs.FileStat)
	for name, node := range target.Children {
		if node.IsDir {
			res[path+name] = &tdfs.FileStat{IsDir: true}
		} else if !node.Meta.IsDeleted && node.Replica > 0 {
			res[path+name] = &tdfs.FileStat{
				IsDir:    false,
				Medatada: &node.Meta,
				Replica:  node.Replica,
			}
		}
	}
	return res, nil
}

func (t *PathTrie) WalkAllFiles(action func(path string, fileNode *INode)) {
	t.doWithAllFiles(t.Root, "", action)
}

func (t *PathTrie) doWithAllFiles(node *INode, currPath string, action func(path string, fileNode *INode)) {
	if node == nil {
		return
	}
	if node.IsDir {
		for childName, nextNode := range node.Children {
			t.doWithAllFiles(nextNode, currPath+"\\"+childName, action)
		}
	} else {
		action(currPath, node)
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
