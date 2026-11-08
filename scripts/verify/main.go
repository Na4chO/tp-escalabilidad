package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	verify2 "github.com/Na4chO/tp-escalabilidad/verify/verify"
)

const (
	dataDir        = "/data"
	outputDir      = "/output"
	expectedDir    = "/output/expected"
	defaultClients = 3
)

func main() {
	clients := flag.Int("clients", defaultClients, "number of output/client_run_<i> directories to verify")
	flag.Parse()

	os.RemoveAll(expectedDir)
	if err := verify2.Generate(dataDir, expectedDir); err != nil {
		fmt.Fprintln(os.Stderr, "generate:", err)
		os.Exit(1)
	}

	failed := false
	for i := 0; i < *clients; i++ {
		name := fmt.Sprintf("client_run_%d", i)
		fmt.Printf("Comparing %s\n", name)
		if verify2.Compare(expectedDir, filepath.Join(outputDir, name), []int{1, 2, 3, 4, 5}) != 0 {
			failed = true
		}
	}

	if failed {
		fmt.Println("Verification failed")
		os.Exit(1)
	}
	fmt.Printf("All %d clients match the expected results\n", *clients)
}
