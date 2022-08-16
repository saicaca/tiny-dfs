package util

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
