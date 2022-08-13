package main

import (
	"fmt"
	"testing"
)

func TestPath(t *testing.T) {
	paths := []string{
		"/a/b/////c",
		"a//b/\\c//",
		"\\\\\\\\a/b\\c",
	}

	for _, value := range paths {
		/*		splitFn := func(c rune) bool {
					return c == '\\' || c == '/'
				}
				fmt.Println(strings.FieldsFunc(value, splitFn))*/

		fmt.Println(beautifyPath(value))
	}
}

func TestDirCreate(t *testing.T) {
	trie := NewPathTrie()
	_ = trie.MkdirAll("a//b//c")
	_ = trie.MkdirAll("a\\b/d")
	_ = trie.MkdirAll("a/b/e")

	trie.List("/a/b")
	trie.List("/")
	trie.List("/a")

}

func TestCreateFile(t *testing.T) {
	meta := MetaData{
		Name: "haha.txt",
		Size: 660,
	}
	trie := NewPathTrie()
	trie.PutFile("aaa/bbb/haha.txt", meta)
	trie.List("aaa/")
	trie.List("aaa/bbb")

}
