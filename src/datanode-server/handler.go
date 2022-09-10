package main

import (
	"context"
	"log"
	"tiny-dfs/gen-go/tdfs"
)

type DataNodeHandler struct {
	core *DataNodeCore
}

func NewDataNodeHandler(core *DataNodeCore) *DataNodeHandler {
	return &DataNodeHandler{
		core: core,
	}
}

func (d *DataNodeHandler) MoveFile(ctx context.Context, old_path string, new_path string, request_time int64) (_err error) {
	err := d.core.Move(old_path, new_path, request_time)
	return err
}

func (d *DataNodeHandler) MakeReplica(ctx context.Context, target_addr string, file_path string) (_r *tdfs.Response, _err error) {
	d.core.MakeReplica(target_addr, file_path)
	return &tdfs.Response{Status: 200, Msg: "MakeReplica ok"}, nil
}

func (d *DataNodeHandler) ReceiveReplica(ctx context.Context, file_path string, file *tdfs.File) (_r *tdfs.Response, _err error) {
	err := d.core.Save(file_path, file.Data, file.Medatada)
	if err != nil {
		log.Println("创建副本时新建文件失败：", err)
		return &tdfs.Response{Status: 500, Msg: "ReceiveReplica Failed"}, err
	}
	return &tdfs.Response{Status: 200, Msg: "ReceiveReplica ok"}, nil
}

func (d *DataNodeHandler) UpdateMetadata(ctx context.Context, path string, metadata *tdfs.Metadata) (_err error) {
	err := d.core.UpdateFile(path, metadata, nil)
	return err
}

func (d *DataNodeHandler) Ping(ctx context.Context) (_r *tdfs.DNStat, _err error) {
	return d.core.GetStat(), nil
}

func (d *DataNodeHandler) Put(ctx context.Context, remote_file_path string, file_data []byte, metadata *tdfs.Metadata) (_r *tdfs.Response, _err error) {
	log.Println("Enter hanlder Put")

	err := d.core.Save(remote_file_path, file_data, metadata)
	if err != nil {
		log.Println(err)
	}
	return &tdfs.Response{Status: 200, Msg: "Put ok"}, nil
}

func (d *DataNodeHandler) Get(ctx context.Context, remote_file_path string) (_r *tdfs.Response, _err error) {
	file, err := d.core.Get(remote_file_path)
	if err != nil {
		return &tdfs.Response{Status: 400}, err
	}
	return &tdfs.Response{Status: 200, File: file}, nil
}

func (d *DataNodeHandler) Delete(ctx context.Context, remote_file_path string) (_r *tdfs.Response, _err error) {
	d.core.Delete(remote_file_path)
	return &tdfs.Response{Status: 200}, nil
}

func (d *DataNodeHandler) PutChunk(ctx context.Context, task_id string, offset int64, data []byte, md5 string) (_r *tdfs.PutChunkResp, _err error) {
	resp, err := d.core.PutChunk(task_id, offset, data, md5)
	return resp, err
}
