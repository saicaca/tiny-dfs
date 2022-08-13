package main

import (
	"errors"
	"fmt"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

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
					fmt.Printf("Put a file from %s to %s\n", localPath, remotePath)
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
					fmt.Printf("Download the file %s to %s\n", remotePath, localPath)
					return nil
				},
			},
			{
				Name:    "delete",
				Aliases: []string{"d"},
				Usage:   "Delete a file on DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("参数数量错误")
					}
					remotePath := c.Args().Get(0)
					fmt.Printf("Delete a file %s\n", remotePath)
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
					fmt.Printf("Show the metadata of file %s\n", remotePath)
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
					fmt.Printf("Create directories %s\n", remotePath)
					return nil
				},
			},
			{
				Name:    "rename",
				Aliases: []string{},
				Usage:   "Rename a file on DFS",
				Flags:   []cli.Flag{},
				Action: func(c *cli.Context) error {
					if c.NArg() != 2 {
						return errors.New("参数数量错误")
					}
					srcPath := c.Args().Get(0)
					destPath := c.Args().Get(1)
					fmt.Printf("Rename %s to %s\n", srcPath, destPath)
					return nil
				},
			},
			{
				Name:    "list",
				Aliases: []string{"ls"},
				Usage:   "List files and sub directories of given directory",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "recursive",
						Aliases: []string{"r"},
						Usage:   "Recursive",
					},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return errors.New("参数数量错误")
					}
					path := c.Args().Get(0)
					if c.Bool("r") {
						fmt.Printf("List every thing recursively in %s\n", path)
					} else {
						fmt.Printf("List every thing in %s\n", path)
					}
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
