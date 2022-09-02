package util

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

func GetNameNodeAddrs() []string {
	paths := []string{
		"config.yml",
		"../config.yml",
	}

	var yfile []byte
	var err error
	for _, path := range paths {
		yfile, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	if err != nil {
		if err != nil {
			log.Println("读取配置文件 config.yml 失败，请检查文件路径")
		}
	}
	data := make(map[interface{}]interface{})
	err2 := yaml.Unmarshal(yfile, &data)
	if err2 != nil {
		fmt.Println("解析配置文件失败", err2)
	}

	ret := make([]string, 0)
	for _, addr := range data["namenode"].([]interface{}) {
		ret = append(ret, addr.(string))
	}
	return ret
}
