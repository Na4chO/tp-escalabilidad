package client

import (
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

const queries = 5

func receiveResults(conn io.Reader, outputPath string) error {
	start := time.Now()
	if err := os.MkdirAll(outputPath, 0o755); err != nil {
		return err
	}
	writers := map[uint8]*csv.Writer{}

	for query := uint8(1); query <= queries; query++ {
		file, err := os.Create(filepath.Join(outputPath, fmt.Sprintf("query%d.csv", query)))
		if err != nil {
			return err
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()
		writers[query] = writer
	}

	rowsByQuery := map[uint8]int{}
	totalRows := 0

	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				attrs := []any{
					"output", outputPath,
					"rows", totalRows,
					"elapsed", time.Since(start).Round(time.Millisecond),
				}
				for query := uint8(1); query <= queries; query++ {
					attrs = append(attrs, fmt.Sprintf("query%d", query), rowsByQuery[query])
				}
				slog.Info("Results written", attrs...)
				return nil
			}
			return err
		}
		if msgType != external.QueryResult {
			return fmt.Errorf("unexpected message type %d", msgType)
		}

		result, err := external.ReadQueryResult(conn)
		if err != nil {
			return err
		}
		writer, ok := writers[result.QueryType]
		if !ok {
			return fmt.Errorf("unexpected query type %d", result.QueryType)
		}
		resultRows := rows(result)
		if err := writer.WriteAll(resultRows); err != nil {
			return err
		}
		rowsByQuery[result.QueryType] += len(resultRows)
		totalRows += len(resultRows)
	}
}

func rows(result inner.QueryResult) [][]string {
	switch result.QueryType {
	case inner.QueryMetadata:
		return [][]string{{result.Url, result.DateModified}}
	case inner.QueryImage:
		return [][]string{{result.Name, result.Url, base64.StdEncoding.EncodeToString(result.Image)}}
	case inner.QueryIntersections:
		var rows [][]string
		for _, word := range result.IntersectionWords {
			rows = append(rows, []string{word})
		}
		return rows
	case inner.QueryTopWords:
		var rows [][]string
		for _, word := range result.TopWords {
			rows = append(rows, []string{word.Word, strconv.FormatUint(uint64(word.Count), 10)})
		}
		return rows
	default:
		return [][]string{{result.Url, strconv.FormatUint(uint64(result.WordCount), 10)}}
	}
}
