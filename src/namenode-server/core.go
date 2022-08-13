package main

import (
	"tiny-dfs/src/model"
)

type (
	MetaData model.MetaData
)

type NameNodeCore struct {
	MetaTrie *PathTrie
}

func NewNameNodeCore() *NameNodeCore {
	core := &NameNodeCore{
		MetaTrie: NewPathTrie(),
	}
	return core
}
