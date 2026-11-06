package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	clients := flag.Int("clients", 3, "number of output/client_run_<i> directories to verify")
	expected := flag.String("expected", "output/expected", "expected results directory")
	flag.Parse()

	root, err := os.Getwd()
	if err != nil {
		fail(err)
	}

	if err := verify(root, *expected, *clients); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

func verify(root, expectedFlag string, clients int) error {
	if clients <= 0 {
		return fmt.Errorf("clients must be greater than zero")
	}

	outputDir := filepath.Join(root, "output")
	expectedDir := expectedFlag
	if !filepath.IsAbs(expectedDir) {
		expectedDir = filepath.Join(root, expectedDir)
	}

	relExpected, err := filepath.Rel(outputDir, expectedDir)
	if err != nil || strings.HasPrefix(relExpected, "..") {
		return fmt.Errorf("expected directory %s must be inside %s", expectedDir, outputDir)
	}

	if _, err := os.Stat(filepath.Join(expectedDir, "files.txt")); err != nil {
		return fmt.Errorf("missing %s; run make expected", filepath.Join(expectedDir, "files.txt"))
	}

	if err := checkFiles(root, expectedDir); err != nil {
		return err
	}

	if err := exec.Command("docker", "image", "inspect", "tp-expected").Run(); err != nil {
		return fmt.Errorf("tp-expected image not found; run make expected")
	}

	failed := false
	for i := 0; i < clients; i++ {
		name := fmt.Sprintf("client_run_%d", i)
		actualDir := filepath.Join(outputDir, name)
		if _, err := os.Stat(actualDir); err != nil {
			fmt.Printf("FAIL %s: missing output directory %s\n", name, actualDir)
			failed = true
			continue
		}

		relActual, err := filepath.Rel(outputDir, actualDir)
		if err != nil {
			return err
		}

		fmt.Printf("Comparing %s\n", name)
		err = run("docker",
			"run", "--rm",
			"-v", outputDir+":/output:ro",
			"tp-expected", "compare",
			filepath.Join("/output", filepath.ToSlash(relExpected)),
			filepath.Join("/output", filepath.ToSlash(relActual)),
			"1", "2", "3", "4", "5",
		)
		if err != nil {
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("verification failed")
	}
	fmt.Printf("All %d clients match the expected results\n", clients)
	return nil
}

func checkFiles(root, expectedDir string) error {
	matches, err := filepath.Glob(filepath.Join(root, "data", "*.parquet"))
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		return fmt.Errorf("no .parquet files in %s", filepath.Join(root, "data"))
	}

	actual := make([]string, len(matches))
	for i, match := range matches {
		actual[i] = filepath.Base(match)
	}
	sort.Strings(actual)

	content, err := os.ReadFile(filepath.Join(expectedDir, "files.txt"))
	if err != nil {
		return err
	}
	var expected []string
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		if line != "" {
			expected = append(expected, line)
		}
	}
	sort.Strings(expected)

	if !equal(actual, expected) {
		return fmt.Errorf("expected results are stale: data/ changed; run make expected")
	}
	return nil
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
