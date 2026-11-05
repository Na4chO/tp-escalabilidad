package main

import (
	"bufio"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const shownDiffs = 5

func compare(expectedDir, actualDir string, queries []int) int {
	failed := false
	for _, query := range queries {
		ok, err := compareQuery(expectedDir, actualDir, query)
		if err != nil {
			fmt.Printf("FAIL query%d: %v\n", query, err)
			ok = false
		}
		failed = failed || !ok
	}
	if failed {
		return 1
	}
	return 0
}

func compareQuery(expectedDir, actualDir string, query int) (bool, error) {
	expected, err := readCSV(filepath.Join(expectedDir, fmt.Sprintf("query%d.csv", query)))
	if err != nil {
		return false, err
	}
	actual, err := readCSV(filepath.Join(actualDir, fmt.Sprintf("query%d.csv", query)))
	if err != nil {
		return false, err
	}

	ok := true
	if query == 2 {
		// Image bytes come from Wikimedia, so only check that there is one
		var withoutImage [][]string
		for i, row := range actual {
			if len(row) < 3 || !hasImage(row[2]) {
				withoutImage = append(withoutImage, row)
			}
			actual[i] = row[:min(2, len(row))]
		}
		if len(withoutImage) > 0 {
			fmt.Printf("FAIL query2: %d rows without image, e.g. %v\n", len(withoutImage), withoutImage[0])
			ok = false
		}
	}

	var missing, extra [][]string
	if query == 4 {
		missing, extra = compareTop(expected, actual)
	} else {
		missing, extra = compareRows(expected, actual)
	}
	if len(missing) > 0 || len(extra) > 0 {
		fmt.Printf("FAIL query%d: expected %d rows, got %d\n", query, len(expected), len(actual))
		for _, row := range missing[:min(shownDiffs, len(missing))] {
			fmt.Printf("       missing %v\n", row)
		}
		for _, row := range extra[:min(shownDiffs, len(extra))] {
			fmt.Printf("       extra   %v\n", row)
		}
		return false, nil
	}
	if ok {
		fmt.Printf("OK   query%d: %d rows\n", query, len(actual))
	}
	return ok, nil
}

func hasImage(encoded string) bool {
	image, err := base64.StdEncoding.DecodeString(encoded)
	return err == nil && len(image) > 0
}

// compareRows compares as multisets, rows can arrive in any order
func compareRows(expected, actual [][]string) (missing, extra [][]string) {
	counts := map[string]int{}
	for _, row := range expected {
		counts[strings.Join(row, "\x00")]++
	}
	for _, row := range actual {
		key := strings.Join(row, "\x00")
		if counts[key] > 0 {
			counts[key]--
			continue
		}
		extra = append(extra, row)
	}
	for _, row := range expected {
		key := strings.Join(row, "\x00")
		if counts[key] > 0 {
			counts[key]--
			missing = append(missing, row)
		}
	}
	return missing, extra
}

// compareTop checks the counts, words tied at the last position can be any of them
func compareTop(expected, actual [][]string) (missing, extra [][]string) {
	expectedCounts, actualCounts := counts(expected), counts(actual)
	if !slices.Equal(expectedCounts, actualCounts) || len(expectedCounts) == 0 {
		return compareRows(expected, actual)
	}
	boundary := expectedCounts[len(expectedCounts)-1]
	return compareRows(aboveCount(expected, boundary), aboveCount(actual, boundary))
}

func counts(rows [][]string) []int {
	var values []int
	for _, row := range rows {
		if len(row) == 2 {
			value, _ := strconv.Atoi(row[1])
			values = append(values, value)
		}
	}
	slices.SortFunc(values, func(a, b int) int { return b - a })
	return values
}

func aboveCount(rows [][]string, boundary int) [][]string {
	var above [][]string
	for _, row := range rows {
		if len(row) != 2 {
			above = append(above, row)
			continue
		}
		if value, _ := strconv.Atoi(row[1]); value > boundary {
			above = append(above, row)
		}
	}
	return above
}

func readCSV(path string) ([][]string, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	reader.FieldsPerRecord = -1
	return reader.ReadAll()
}
