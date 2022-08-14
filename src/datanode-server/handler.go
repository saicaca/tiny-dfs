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

func (d *DataNodeHandler) Ping(ctx context.Context) (_r *tdfs.Response, _err error) {
	log.Println("Ping Success")
	return &tdfs.Response{Status: 200}, nil
}

func (d *DataNodeHandler) Put(ctx context.Context, remote_file_path string, file_data []byte, metadata *tdfs.Metadata) (_r *tdfs.Response, _err error) {
	log.Println("Enter hanlder Put")

	err := d.core.Save(remote_file_path, file_data, metadata)
	if err != nil {
		log.Println(err)
	}
	return &tdfs.Response{Status: 200, Msg: "Put ok"}, nil
}

func (d *DataNodeHandler) Get(ctx context.Context, remote_file_path string, local_file_path string) (_r *tdfs.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (d *DataNodeHandler) Delete(ctx context.Context, remote_file_path string) (_r *tdfs.Response, _err error) {
	//TODO implement me
	panic("implement me")
}
