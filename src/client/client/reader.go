package client

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// errStopped indica que la lectura se cortó porque el cliente se está cerrando.
var errStopped = errors.New("client stopped")

// batchHandler recibe cada batch de artículos ya proyectados y parseados.
type batchHandler func(batch []Article) error

// readParquetFile lee un .parquet de a batchSize filas. Por cada batch:
// lee solo las columnas proyectadas, parsea cada fila y llama a handle.
// La memoria queda acotada a un batch, sin importar el tamaño del archivo.
func readParquetFile(path string, batchSize int, keepRunning func() bool, handle batchHandler) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Abre el footer, cruza el esquema del archivo con rawArticle y queda
	// listo para leer solo esas columnas. *os.File le da el tamaño (Stat).
	reader := parquet.NewGenericReader[rawArticle](file)
	defer reader.Close()

	rawBatch := make([]rawArticle, batchSize)
	articles := make([]Article, 0, batchSize)

	for {
		if !keepRunning() {
			return errStopped
		}

		clear(rawBatch) // que no queden punteros del batch anterior
		n, readErr := reader.Read(rawBatch)

		articles = articles[:0]
		for i := range n {
			article, err := parseArticle(&rawBatch[i])
			if err != nil {
				slog.Warn("Discarding article", "err", err)
				continue
			}
			articles = append(articles, article)
		}
		if len(articles) > 0 {
			if err := handle(articles); err != nil {
				return err
			}
		}

		// El último Read puede devolver filas Y io.EOF: primero se procesan.
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("reading %s: %w", path, readErr)
		}
	}
}

// listParquetFiles devuelve los .parquet de path (un archivo, o un directorio
// recorrido recursivamente), ordenados para que cada corrida sea igual.
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
	sort.Strings(files)
	return files, err
}
