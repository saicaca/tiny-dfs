package main

import (
	"context"
	"errors"
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

func (n NameNodeHandler) RegisterDeprecated(ctx context.Context, meta_map map[string]*tdfs.Metadata, client_ip string) (_r *tdfs.Response, _err error) {
	log.Println("已连接 DataNode", client_ip)
	//addr, ok := thrift.GetHeader(ctx, "addr")
	//if !ok {
	//	fmt.Println("failed to get addr from context")
	//}
	//log.Println("Get addr from context:", addr)
	//log.Println("Get RHeader List:", thrift.GetReadHeaderList(ctx))
	//log.Println("Get WHeader List:", thrift.GetWriteHeaderList(ctx))
	//n.core.PutFileLegacy(meta_map, client_ip)

	go n.core.Registry.PutDataNode(client_ip)

	return &tdfs.Response{Status: 200, Msg: "Register success"}, nil
}

func (n NameNodeHandler) Register(ctx context.Context, chunks []string, datanode_ip string) (_err error) {
	n.core.ReceiveChunks(chunks, datanode_ip)
	n.core.Registry.PutDataNode(datanode_ip)
	return
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

func (n NameNodeHandler) Stat(ctx context.Context, remote_file_path string) (_r *tdfs.FileStat, _err error) {
	node := n.core.MetaTrie.GetFileNode(remote_file_path)
	if node == nil {
		return nil, errors.New("文件 " + remote_file_path + "不存在")
	}
	return &tdfs.FileStat{IsDir: false, Medatada: &node.Meta, Replica: node.Replica}, nil
}

func (n NameNodeHandler) List(ctx context.Context, remote_dir_path string) (_r map[string]*tdfs.FileStat, _err error) {
	res, err := n.core.MetaTrie.ListStat(remote_dir_path)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (n NameNodeHandler) ListDataNode(ctx context.Context) (_r map[string]*tdfs.DNStat, _err error) {
	return n.core.Registry.GetDNStats(), nil
}

func (n NameNodeHandler) Put(ctx context.Context, path string, metadata *tdfs.Metadata, client_ip string) (_r *tdfs.Response, _err error) {
	n.core.PutSingleFile(path, metadata, client_ip)
	return &tdfs.Response{Status: 200, Msg: "Put success"}, nil
}

func (n NameNodeHandler) Delete(ctx context.Context, remote_file_path string) (_err error) {
	err := n.core.SetDeleted(remote_file_path)
	if err != nil {
		log.Println("删除文件", remote_file_path, "失败：", err)
	}
	return err
}

func (n NameNodeHandler) Mkdir(ctx context.Context, remote_file_path string) (_err error) {
	err := n.core.MetaTrie.MkdirAll(remote_file_path)
	return err
}

func (n NameNodeHandler) Rename(ctx context.Context, rename_src_path string, rename_dest_path string) (_err error) {
	err := n.core.Move(rename_src_path, rename_dest_path)
	return err
}

func (n NameNodeHandler) InitializePut(ctx context.Context, file_path string, metadata *tdfs.Metadata, total_block int64) (taskId string, _err error) {
	taskId, _err = n.core.InitializePut(file_path, metadata, total_block)
	return
}

func (n NameNodeHandler) PutChunk(ctx context.Context, task_id string, seq int64, chunk_id string) (_r *tdfs.PutChunkResp, _err error) {
	_r, _err = n.core.PutChunk(task_id, seq, chunk_id)
	return
}

func (n NameNodeHandler) GetChunkList(ctx context.Context, path string, offset int64, size int64) (_r *tdfs.ChunkList, _err error) {
	//TODO implement me
	panic("implement me")
}
