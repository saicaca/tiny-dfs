package util

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

func GetKeys(m map[string]interface{}) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}

func GetValues(m map[string]interface{}) interface{} {
	values := make([]interface{}, len(m))
	i := 0
	for k := range m {
		values[i] = m[k]
		i++
	}
	return values
}

func FormatSize(bytes int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	res := float64(bytes)
	i := 0
	for res > 1024 {
		res /= 1024.0
		i++
	}
	return fmt.Sprintf("%.2f %s", res, units[i])
}

func SizeToByte(str string) int64 {
	units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	i := 0
	runes := []rune(str)
	for unicode.IsLetter(runes[len(runes)-1-i]) {
		i++
	}
	num, err := strconv.ParseFloat(str[:len(str)-i], 64)
	if err != nil {
		return -1
	}
	unitStr := strings.ToUpper(str[len(str)-i:])
	i = 0
	for i = range units {
		if units[i] == unitStr {
			break
		}
		i++
	}
	if i == len(units) {
		return -1
	}
	for i > 0 {
		num *= 1024
		i--
	}
	return int64(num)
}

func FormatAddress(addr string) string {
	return strings.ReplaceAll(addr, "localhost", "127.0.0.1")
}
