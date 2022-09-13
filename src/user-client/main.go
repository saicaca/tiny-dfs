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

var nameNodeAddr string

func main() {
	getNameNodeAddr()

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
					getFileDeprecated(remotePath, localPath)
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

func putFileDeprecated(localPath string, remotePath string) {
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
		//client, err := dnc.NewDataNodeClient(DNAddr)
		fmt.Println("正在上传", localPath, "到 DataNode", DNAddr, remotePath)

		if err != nil {
			log.Println("Failed to connect DataNode", DNAddr)
			continue
		}

		//_, err = client.Put(defaultCtx, remotePath, data, meta)
		dnc.RequestDataNode(DNAddr, func(client *tdfs.DataNodeClient) {
			_, err := client.Put(context.Background(), remotePath, data, meta)
			if err != nil {
				panic(err)
			}
		})
		if err != nil {
			log.Println("Put file error:", err)
			continue
		} else {
			fmt.Println("成功上传", localPath, "到 DataNode", DNAddr)
			break
		}
	}
}

func putFile(localPath string, remotePath string) {
	// get chunker
	chunker, err := NewFileChunker(localPath)
	if err != nil {
		panic(err)
	}

	// create metadata
	meta := &tdfs.Metadata{
		Size: int64(chunker.fileSize),
	}

	// call namenode to create put task
	var taskId string
	nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
		taskId, err = client.InitializePut(context.Background(), remotePath, meta, int64(chunker.total))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// put chunks
	const MAX_RETRY = 5
	seq := 0
	for chunker.HasNext() {
		fmt.Printf("Putting chunk %v of %v\n", seq+1, chunker.total)
		chunkData := chunker.GetNext()
		md5 := util.Md5Str(chunkData)
		failCount := 0
		var dataNodes []string
		for failCount < MAX_RETRY {
			// get datanode addresses to put chunk
			nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
				dataNodes, err = client.GetSpareNodes(context.Background())
				return nil
			})
			if err != nil {
				fmt.Println("failed to get DataNodes")
				failCount++
				continue
			}

			// put chunk to datanode
			_ = dnc.RequestDataNode(dataNodes[0], func(client *tdfs.DataNodeClient) {
				_, err = client.PutChunk(context.Background(), taskId, int64(seq), chunkData, md5)
			})
			if err != nil {
				fmt.Println("failed to put chunk to DataNode " + dataNodes[0])
				failCount++
				continue
			} else {
				break
			}
		}

		if failCount == MAX_RETRY { // failed to put chunk
			panic(errors.New(fmt.Sprintf("failed to put chunk %v", seq)))
		} else { // succeeded to put chunk
			seq++
		}
	}

	fmt.Println("Succeed to put file", localPath, "to", remotePath)
}

func getFileDeprecated(remotePath string, localPath string) {

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
		log.Println("正在从", DNAddr, "下载文件")
		dnc.RequestDataNode(DNAddr, func(client *tdfs.DataNodeClient) {
			resp, err = client.Get(context.Background(), remotePath)
		})
		if err != nil {
			fmt.Println("从", DNAddr, "下载", remotePath, "失败：", err)
		} else {
			fmt.Println("从 DataNodes", DNAddr, "下载", remotePath, "到", localPath)
			break
		}
	}
	if err != nil {
		panic("下载文件" + remotePath + "失败" + err.Error())
	}

	err = os.WriteFile(localPath, resp.File.Data, 0777)
	if err != nil {
		log.Panicln("Failed to write file:", err)
	}
}

func getFile(remotePath string, localPath string) {
	// ensure local directories exist
	dir := filepath.Dir(localPath)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Panicln("Failed to create directories", localPath, err)
	}

	// get chunk IDs

}

func deleteFile(remotePath string) {
	var err error
	nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
		err = client.Delete(context.Background(), remotePath)
		return nil
	})

	if err != nil {
		fmt.Println("删除文件失败：", err)
	} else {
		fmt.Println("成功删除文件", remotePath)
	}
}

func move(oldPath string, newPath string) {
	var err error
	nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
		err = client.Rename(context.Background(), oldPath, newPath)
		return nil
	})

	if err != nil {
		fmt.Println("移动/重命名文件失败：", err)
	} else {
		fmt.Println("成功将", oldPath, "移动到 / 重命名为", newPath)
	}
}

func stat(path string) {
	var stat *tdfs.FileStat
	var err error
	nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
		stat, err = client.Stat(context.Background(), path)
		return nil
	})

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
	var stats map[string]*tdfs.FileStat
	var err error

	nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
		stats, err = client.List(context.Background(), path)
		return nil
	})
	if err != nil {
		fmt.Println("获取目录信息失败：", err)
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"名称", "类型", "修改时间", "文件大小", "副本数量"})
	for p, stat := range stats {
		if stat.IsDir {
			t.AppendRow(table.Row{p, "目录", "--", "--", "--"})
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
	//nnClient := getNameNodeClient()
	//mp, err := nnClient.ListDataNode(context.Background())
	var mp map[string]*tdfs.DNStat
	var err error

	nnc.RequestNameNode(nameNodeAddr, func(client *tdfs.NameNodeClient) error {
		mp, err = client.ListDataNode(context.Background())
		if err != nil {
			return err
		}
		return nil
	})

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

func getNameNodeAddr() {
	yfile, err := os.ReadFile("config.yml")
	if err != nil {
		fmt.Println("未找到配置文件 config.yml")
	}
	data := make(map[interface{}]interface{})
	err2 := yaml.Unmarshal(yfile, &data)
	if err2 != nil {
		fmt.Println("解析配置文件失败", err2)
	}

	for _, addr := range data["namenode"].([]interface{}) {
		nameNodeAddr = addr.(string)
		break
	}
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
