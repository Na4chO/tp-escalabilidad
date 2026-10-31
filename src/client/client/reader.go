package client

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Na4chO/tp-escalabilidad/common/article"
	"github.com/parquet-go/parquet-go"
)

var errStopped = errors.New("client stopped")

type articleHandler func(a article.Article) error

func readParquetFile(path string, keepRunning func() bool, handle articleHandler) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewReader(file)
	defer reader.Close()

	for keepRunning() {
		row := map[string]any{}
		if err := reader.Read(&row); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("reading %s: %w", path, err)
		}
		if err := handle(article.FromRow(row)); err != nil {
			return err
		}
	}
	return errStopped
}

func listParquetFiles(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{path}, nil
	}

	var files []string
	err = filepath.WalkDir(path, func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
			files = append(files, p)
		}
		return nil
	})
	return files, err
}
