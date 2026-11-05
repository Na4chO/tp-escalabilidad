// Reference implementation of the 5 queries, used to check the results of the system.
//
//	expected generate <data_dir> <output_dir>
//	expected compare <expected_dir> <actual_dir> <query> [<query> ...]
//
// Both directories hold query<N>.csv files with the format the client writes.
// It doesn't import anything from the system, so a bug there can't hide itself here.
package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) >= 4 && os.Args[1] == "generate" {
		if err := generate(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintln(os.Stderr, "generate:", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) >= 5 && os.Args[1] == "compare" {
		var queries []int
		for _, arg := range os.Args[4:] {
			query, err := strconv.Atoi(arg)
			if err != nil {
				fmt.Fprintln(os.Stderr, "invalid query:", arg)
				os.Exit(2)
			}
			queries = append(queries, query)
		}
		os.Exit(compare(os.Args[2], os.Args[3], queries))
	}
	fmt.Fprintln(os.Stderr, "usage: expected generate <data_dir> <output_dir> | expected compare <expected_dir> <actual_dir> <query>...")
	os.Exit(2)
}
