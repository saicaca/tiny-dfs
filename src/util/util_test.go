package util

import (
	"fmt"
	"testing"
)

func TestName(t *testing.T) {
	mp := make(map[string]string)
	mp["hello"] = "world"
	mp["nihao"] = "shijie"

}

func TestSizeConvert(t *testing.T) {
	fmt.Println(FormatSize(1024 * 1024 * 4))
}
