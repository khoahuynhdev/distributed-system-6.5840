package main

import (
	"fmt"
	"path/filepath"
)

func main() {
	matches, err := filepath.Glob("mr-*-0")
	if err != nil {
		fmt.Errorf("got error", err)
	}
	for _, match := range matches {
		fmt.Println(match)
	}
}
