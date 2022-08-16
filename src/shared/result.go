package shared

type Result struct {
	Data map[string]interface{}
}

func NewResult() *Result {
	return &Result{
		Data: make(map[string]interface{}),
	}
}
