package main

import (
	"fmt"
	"testing"
	"tiny-dfs/gen-go/tdfs"
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
	meta := &tdfs.Metadata{
		Name: "haha.txt",
		Size: 660,
	}
	trie := NewPathTrie()
	trie.PutFile("aaa/bbb/haha.txt", "192.168.0.114", meta)
	trie.List("aaa/")
	trie.List("aaa/bbb")
}

func TestSliceMap(t *testing.T) {
	mp := make(map[string][]string)
	mp["114514"] = append(mp["114514"], "ipaddr")
	mp["114514"] = append(mp["114514"], "second")
	fmt.Println(mp["114514"])
}

func TestSlice(t *testing.T) {
	var sl []string
	sl = append(sl, "114514")
	fmt.Println(sl)
}
