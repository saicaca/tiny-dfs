package util

import (
	"fmt"
	"testing"
)

func TestIsLocalHost(t *testing.T) {
	fmt.Println(IsLocalHost("localhost"))
	fmt.Println(IsLocalHost("127.0.0.2"))
	fmt.Println(IsLocalHost("127.0.0.1"))
}
