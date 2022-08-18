package util

import "fmt"

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
