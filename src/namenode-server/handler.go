package main

import (
	"context"
	"tiny-dfs/gen-go/NameNode"
)

type NameNodeHandler struct {
	core *NameNodeCore
}

func NewNameNodeHandler(core *NameNodeCore) *NameNodeHandler {
	return &NameNodeHandler{
		core: core,
	}
}

func (n NameNodeHandler) Put(ctx context.Context, local_file_path string, remote_file_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Get(ctx context.Context, remote_file_path string, local_file_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Delete(ctx context.Context, remote_file_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Stat(ctx context.Context, remote_file_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Mkdir(ctx context.Context, remote_file_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) Rename(ctx context.Context, rename_src_path string, rename_dest_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}

func (n NameNodeHandler) List(ctx context.Context, remote_dir_path string) (_r *NameNode.Response, _err error) {
	//TODO implement me
	panic("implement me")
}
