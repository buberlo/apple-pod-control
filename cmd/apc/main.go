package main

import (
	"fmt"
	"os"

	"github.com/buberlo/apple-pod-control/internal/cli"
)

func main() {
	command := cli.NewCommand(os.Stdout, os.Stderr)
	if err := command.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
