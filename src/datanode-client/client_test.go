package dnc

import (
	"context"
	"fmt"
	"testing"
	"tiny-dfs/gen-go/tdfs"
)

var defaultCtx = context.Background()

func TestCreate(t *testing.T) {
	var client *tdfs.DataNodeClient
	client, err := NewDataNodeClient("localhost:9090")
	if err != nil {
		t.Error("failed to create client")
	}
	resp, err := client.Ping(defaultCtx)
	if err != nil {
		fmt.Println("ping error:", err)
	} else {
		fmt.Println(resp)
	}
}
