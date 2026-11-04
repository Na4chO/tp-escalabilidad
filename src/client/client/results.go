package client

import (
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

const queries = 5

func receiveResults(conn io.Reader, outputPath string) error {
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

	for {
		msgType, err := external.ReadMsgType(conn)
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
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
		if err := writer.WriteAll(rows(result)); err != nil {
			return err
		}
	}
}

func rows(result inner.QueryResult) [][]string {
	switch result.QueryType {
	case 1:
		return [][]string{{result.Url, result.DateModified}}
	case 2:
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
