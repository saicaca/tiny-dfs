package util

import (
	"crypto/md5"
	"fmt"
)

func Md5Str(data []byte) string {
	sum := md5.Sum(data)
	return fmt.Sprintf("%x", sum)
}
