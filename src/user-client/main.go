package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
	"time"
	"tiny-dfs/gen-go/tdfs"
	dnc "tiny-dfs/src/datanode-client"
	nnc "tiny-dfs/src/namenode-client"
	"tiny-dfs/src/util"
)

var defaultCtx = context.Background()

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:    "put",
				Aliases: []string{"p"},
				Usage:   "Put a local file to DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 2 {
						return errors.New("参数数量错误")
					}
					localPath := c.Args().Get(0)
					remotePath := c.Args().Get(1)
					//fmt.Printf("Put a file from %s to %s\n", localPath, remotePath)
					putFile(localPath, remotePath)
					return nil
				},
			},
			{
				Name:    "get",
				Aliases: []string{"g"},
				Usage:   "Download a file from DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 2 {
						return errors.New("参数数量错误")
					}
					remotePath := c.Args().Get(0)
					localPath := c.Args().Get(1)
					//fmt.Printf("Download the file %s to %s\n", remotePath, localPath)
					getFile(remotePath, localPath)
					return nil
				},
			},
			{
				Name:    "delete",
				Aliases: []string{"d", "rm"},
				Usage:   "Delete a file on DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("参数数量错误")
					}
					remotePath := c.Args().Get(0)
					deleteFile(remotePath)
					//fmt.Printf("Delete a file %s\n", remotePath)
					return nil
				},
			},
			{
				Name:    "stat",
				Aliases: []string{"s"},
				Usage:   "Show the metadata of certain file",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("参数数量错误")
					}
					remotePath := c.Args().Get(0)
					//fmt.Printf("Show the metadata of file %s\n", remotePath)
					stat(remotePath)
					return nil
				},
			},
			{
				Name:    "mkdir",
				Aliases: []string{},
				Usage:   "Create directories on DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("参数数量错误")
					}
					remotePath := c.Args().Get(0)
					//fmt.Printf("Create directories %s\n", remotePath)
					mkdirAll(remotePath)
					return nil
				},
			},
			{
				Name:    "move",
				Aliases: []string{"mv", "rename"},
				Usage:   "Move a file on DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 2 {
						return errors.New("参数数量错误")
					}
					srcPath := c.Args().Get(0)
					destPath := c.Args().Get(1)
					//fmt.Printf("Rename %s to %s\n", srcPath, destPath)
					move(srcPath, destPath)
					return nil
				},
			},
			{
				Name:    "list",
				Aliases: []string{"ls"},
				Usage:   "List files and sub directories of given directory",
				Flags:   []cli.Flag{
					//&cli.BoolFlag{
					//	Name:    "recursive",
					//	Aliases: []string{"r"},
					//	Usage:   "Recursive",
					//},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("参数数量错误")
					}
					path := c.Args().Get(0)
					//if c.Bool("r") {
					//	fmt.Printf("List every thing recursively in %s\n", path)
					//} else {
					//	fmt.Printf("List every thing in %s\n", path)
					//}
					list(path)
					return nil
				},
			},
			{
				Name:    "datanodes",
				Aliases: []string{"dn"},
				Usage:   "List all the DataNodes",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					listDataNodes()
					return nil
				},
			},
		},
		UseShortOptionHandling: true,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalln(err)
	}
}

func putFile(localPath string, remotePath string) {
	nnClient := getNameNodeClient()
	nodes, err := nnClient.GetSpareNodes(context.Background())
	if err != nil {
		fmt.Println("获取 DataNodes 列表失败：", err)
		return
	}

	data, err := os.ReadFile(localPath)
	if err != nil {
		log.Println("Failed to load local file", localPath)
		return
	}

	file, err := os.Open(localPath)
	info, err := file.Stat()
	if err != nil {
		log.Println("Failed to load file info")
		return
	}

	meta := &tdfs.Metadata{
		IsDeleted: false,
		Name:      filepath.Base(remotePath),
		Mtime:     time.Now().UnixMilli(),
		Size:      info.Size(),
	}

	for _, DNAddr := range nodes {
		client, err := dnc.NewDataNodeClient(DNAddr)
		if err != nil {
			log.Println("Failed to connect DataNode", DNAddr)
			continue
		}

		_, err = client.Put(defaultCtx, remotePath, data, meta)
		if err != nil {
			log.Println("Put file error:", err)
			continue
		} else {
			fmt.Println("成功上传", localPath, "到 DataNode", DNAddr)
			break
		}
	}
}

func getFile(remotePath string, localPath string) {

	// 确保本地文件夹存在
	dir := filepath.Dir(localPath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Panicln("创建本地文件夹失败:", err)
	}

	// 获取存有本文件的 DataNode 列表
	nnClient := getNameNodeClient()
	nodes, err := nnClient.GetDataNodesWithFile(context.Background(), remotePath)
	if err != nil {
		fmt.Println("获取 DataNodes 列表失败：", err)
		return
	}

	// 尝试下载文件
	var resp *tdfs.Response
	for _, DNAddr := range nodes {
		client, err := dnc.NewDataNodeClient(DNAddr)
		if err != nil {
			log.Println("Failed to connect DataNode", DNAddr)
		}
		//log.Println("Connected to DataNode", DNAddr)

		resp, err = client.Get(defaultCtx, remotePath)
		if err != nil {
			log.Panicln("Failed to get file:", err)
		} else {
			fmt.Println("从 DataNodes", DNAddr, "下载", remotePath, "到", localPath)
			break
		}
	}

	err = os.WriteFile(localPath, resp.File.Data, 0777)
	if err != nil {
		log.Panicln("Failed to write file:", err)
	}
}

func deleteFile(remotePath string) {
	nnClient := getNameNodeClient()
	err := nnClient.Delete(context.Background(), remotePath)
	if err != nil {
		fmt.Println("删除文件失败：", err)
	}
}

func move(oldPath string, newPath string) {
	nnClient := getNameNodeClient()
	err := nnClient.Rename(context.Background(), oldPath, newPath)
	if err != nil {
		fmt.Println("移动/重命名文件失败：", err)
	}
}

func stat(path string) {
	nnClient := getNameNodeClient()
	stat, err := nnClient.Stat(context.Background(), path)
	if err != nil {
		fmt.Println(err)
		return
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"文件名称", "修改时间", "文件大小", "副本数量"})
	t.AppendRow(table.Row{
		stat.Medatada.Name, time.UnixMilli(stat.Medatada.Mtime).String(), util.FormatSize(stat.Medatada.Size), stat.Replica,
	})
	t.Render()
}

func list(path string) {
	nnClient := getNameNodeClient()
	stats, err := nnClient.List(context.Background(), path)
	if err != nil {
		fmt.Println("获取目录信息失败：", err)
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"名称", "类型", "修改时间", "文件大小", "副本数量"})
	for p, stat := range stats {
		if stat.IsDir {
			t.AppendRow(table.Row{p, "目录", "", "", ""})
		}
	}
	t.AppendSeparator()
	for p, stat := range stats {
		if !stat.IsDir {
			t.AppendRow(table.Row{
				p,
				"文件",
				time.UnixMilli(stat.Medatada.Mtime).String(),
				util.FormatSize(stat.Medatada.Size),
				stat.Replica,
			})
		}
	}
	t.Render()
}

func mkdirAll(path string) {
	nnClient := getNameNodeClient()
	err := nnClient.Mkdir(context.Background(), path)
	if err != nil {
		fmt.Println("创建目录失败，路径上存在同名文件：", err)
		return
	}
	fmt.Println("路径", path, "可用")
}

func listDataNodes() {
	nnClient := getNameNodeClient()
	mp, err := nnClient.ListDataNode(context.Background())
	if err != nil {
		fmt.Println("获取 DataNodes 信息失败：", err)
		return
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "运行地址", "最近活动时间", "文件数量", "已用空间", "总分配空间", "空间使用率", "流量"})
	i := 1
	for addr, stat := range mp {
		t.AppendRow(table.Row{
			i,
			addr,
			time.UnixMilli(stat.StartTime).String(),
			stat.FileNum,
			util.FormatSize(stat.UsedSpace),
			util.FormatSize(stat.TotalSpace),
			fmt.Sprintf("%.2f %%", float64(stat.UsedSpace)/float64(stat.TotalSpace)*100.0),
			util.FormatSize(stat.Traffic),
		})
		i++
	}
	t.Render()
}

func getNameNodeClient() *tdfs.NameNodeClient {
	yfile, err := os.ReadFile("config.yml")
	if err != nil {
		fmt.Println("未找到配置文件 config.yml")
		return nil
	}
	data := make(map[interface{}]interface{})
	err2 := yaml.Unmarshal(yfile, &data)
	if err2 != nil {
		fmt.Println("解析配置文件失败", err2)
		return nil
	}

	var client *tdfs.NameNodeClient
	for _, addr := range data["namenode"].([]interface{}) {
		client, err = nnc.NewNameNodeClient(addr.(string))
		if client == nil || err != nil {
			continue
		} else {
			break
		}
	}
	return client
}
