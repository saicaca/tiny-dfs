package main

import (
	"context"
	"tiny-dfs/gen-go/DataNode"
)

type DataNodeHandler struct {
}

func NewDataNodeHandler() *DataNodeHandler {
	return &DataNodeHandler{}
}

func (d DataNodeHandler) Put(ctx context.Context, local_file_path string, remote_file_path string) (_r *DataNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (d DataNodeHandler) Get(ctx context.Context, remote_file_path string, local_file_path string) (_r *DataNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (d DataNodeHandler) Delete(ctx context.Context, remote_file_path string) (_r *DataNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}
