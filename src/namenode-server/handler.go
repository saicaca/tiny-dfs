package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"tiny-dfs/gen-go/tdfs"
)

type NameNodeHandler struct {
	core *NameNodeCore
}

func NewNameNodeHandler(core *NameNodeCore) *NameNodeHandler {
	return &NameNodeHandler{
		core: core,
	}
}

func (n NameNodeHandler) Register(ctx context.Context, meta_map map[string]*tdfs.Metadata, client_ip string) (_r *tdfs.Response, _err error) {
	fmt.Println("获取到客户端 IP", client_ip)
	n.core.PutFile(meta_map, client_ip)

	go n.core.Registry.PutDataNode(client_ip)

	return &tdfs.Response{Status: 200, Msg: "Register success"}, nil
}

func (n NameNodeHandler) GetSpareNodes(ctx context.Context) (_r []string, _err error) {
	nodes := n.core.Registry.GetSpareDataNodes()
	return nodes, nil
}

func (n NameNodeHandler) GetDataNodesWithFile(ctx context.Context, file_path string) (_r []string, _err error) {
	fileNode := n.core.MetaTrie.GetFileNode(file_path)
	if fileNode == nil || fileNode.Meta.IsDeleted {
		return nil, errors.New("文件" + file_path + "不存在")
	}
	if fileNode.IsDir {
		return nil, errors.New(file_path + "为目录而非文件")
	}

	nodeSet := fileNode.DNList
	nodes := make([]string, 0)
	for node, _ := range nodeSet {
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (n NameNodeHandler) UpdateMetadata(ctx context.Context, file_path string, metadata *tdfs.Metadata) (_err error) {
	err := n.core.UpdateMetadata(file_path, metadata)
	if err != nil {
		log.Println("更新", file_path, "元数据失败：", err)
	}
	return err
}

func (n NameNodeHandler) Put(ctx context.Context, path string, metadata *tdfs.Metadata, client_ip string) (_r *tdfs.Response, _err error) {
	n.core.PutSingleFile(path, metadata, client_ip)
	return &tdfs.Response{Status: 200, Msg: "Put success"}, nil
}

func (n NameNodeHandler) Get(ctx context.Context, remote_file_path string, local_file_path string) (_r *tdfs.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Delete(ctx context.Context, remote_file_path string) (_err error) {
	err := n.core.SetDeleted(remote_file_path)
	if err != nil {
		log.Println("删除文件", remote_file_path, "失败：", err)
	}
	return err
}

func (n NameNodeHandler) Stat(ctx context.Context, remote_file_path string) (_r *tdfs.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Mkdir(ctx context.Context, remote_file_path string) (_err error) {
	err := n.core.MetaTrie.MkdirAll(remote_file_path)
	return err
}

func (n NameNodeHandler) Rename(ctx context.Context, rename_src_path string, rename_dest_path string) (_r *tdfs.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) List(ctx context.Context, remote_dir_path string) (_r *tdfs.Response, _err error) {
	//TODO implement me
	panic("implement me")
}
