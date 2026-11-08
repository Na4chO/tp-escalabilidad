package verify

import (
	"bufio"
	"embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

const (
	topSize     = 20
	lastOldYear = 2020
	readBatch   = 1000
)

var (
	wordRegex   = regexp.MustCompile(`[\p{L}\p{N}_]+`)
	numberRegex = regexp.MustCompile(`^[+-]?(([0-9]+\.?[0-9]*)|(\.[0-9]+))$`)
)

// NLTK stopword lists, the ones the system has to match
//
//go:embed stopwords/english stopwords/french
var stopwordFiles embed.FS

type article struct {
	Url          string `parquet:"url,optional"`
	Name         string `parquet:"name,optional"`
	Abstract     string `parquet:"abstract,optional"`
	DateModified int64  `parquet:"date_modified,optional"`
	InLanguage   *struct {
		Identifier string `parquet:"identifier,optional"`
	} `parquet:"in_language,optional"`
	Image *struct {
		ContentUrl string `parquet:"content_url,optional"`
	} `parquet:"image,optional"`
	Sections string `parquet:"sections,optional"`
}

type candidate struct {
	name, url, image string
}

type wordsCount struct {
	url   string
	count int
}

type results struct {
	q1         [][]string
	q2         []candidate
	newCounts  []wordsCount
	vocabulary map[string]map[string]struct{}
	frequency  map[string]int
	oldWords   int
	oldCount   int
}

func Generate(dataDir, outputDir string) error {
	files, err := parquetFiles(dataDir)
	if err != nil {
		return err
	}
	stopwords, err := loadStopwords()
	if err != nil {
		return err
	}

	r := &results{
		vocabulary: map[string]map[string]struct{}{"en": {}, "fr": {}},
		frequency:  map[string]int{},
	}
	for _, path := range files {
		if err := r.addFile(path, stopwords); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}

	q2, err := r.existingImages(imageCachePath(dataDir))
	if err != nil {
		return err
	}

	average := 0.0
	if r.oldCount > 0 {
		average = float64(r.oldWords) / float64(r.oldCount)
	}
	queries := map[int][][]string{
		1: r.q1,
		2: q2,
		3: r.intersections(),
		4: r.top(),
		5: r.aboveAverage(average),
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	fmt.Printf("Expected results from %d files (old average %.4f):\n", len(files), average)
	for query := 1; query <= 5; query++ {
		if err := writeCSV(filepath.Join(outputDir, fmt.Sprintf("query%d.csv", query)), queries[query]); err != nil {
			return err
		}
		fmt.Printf("  query%d.csv: %d rows\n", query, len(queries[query]))
	}
	return nil
}

func (r *results) addFile(path string, stopwords map[string]bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[article](file)
	defer reader.Close()
	batch := make([]article, readBatch)
	for {
		n, err := reader.Read(batch)
		for _, a := range batch[:n] {
			r.add(a, stopwords)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (r *results) add(a article, stopwords map[string]bool) {
	language := ""
	if a.InLanguage != nil {
		language = a.InLanguage.Identifier
	}
	image := ""
	if a.Image != nil {
		image = a.Image.ContentUrl
	}
	date := time.UnixMilli(a.DateModified).UTC()
	abstractWords := words(a.Abstract, stopwords)

	for _, word := range abstractWords {
		r.frequency[word]++
		if vocabulary, ok := r.vocabulary[language]; ok {
			vocabulary[word] = struct{}{}
		}
	}

	if date.Year() > lastOldYear {
		r.newCounts = append(r.newCounts, wordsCount{url: a.Url, count: len(abstractWords)})
		return
	}
	r.oldWords += len(abstractWords)
	r.oldCount++
	if language == "en" {
		r.q1 = append(r.q1, []string{a.Url, date.Format(time.RFC3339)})
	}
	if image != "" && hasBiography(a.Sections) {
		r.q2 = append(r.q2, candidate{name: a.Name, url: a.Url, image: image})
	}
}

// words returns the unique lowercase words of an abstract, without stopwords nor numbers
func words(abstract string, stopwords map[string]bool) []string {
	seen := map[string]bool{}
	var unique []string
	for _, word := range wordRegex.FindAllString(strings.ToLower(abstract), -1) {
		if stopwords[word] || numberRegex.MatchString(word) || seen[word] {
			continue
		}
		seen[word] = true
		unique = append(unique, word)
	}
	return unique
}

func hasBiography(rawSections string) bool {
	var sections []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(rawSections), &sections); err != nil {
		return false
	}
	for _, section := range sections {
		name := strings.ToLower(strings.TrimSpace(section.Name))
		if name == "biography" || name == "biographie" {
			return true
		}
	}
	return false
}

func (r *results) existingImages(cachePath string) ([][]string, error) {
	cache := loadImageCache(cachePath)
	var rows [][]string
	for _, c := range r.q2 {
		if !imageExists(c.image, cache) {
			fmt.Printf("  query2: skipping %s, image not found: %s\n", c.url, c.image)
			continue
		}
		rows = append(rows, []string{c.name, c.url})
	}
	return rows, saveImageCache(cachePath, cache)
}

func (r *results) intersections() [][]string {
	var rows [][]string
	for word := range r.vocabulary["en"] {
		if _, ok := r.vocabulary["fr"][word]; ok {
			rows = append(rows, []string{word})
		}
	}
	slices.SortFunc(rows, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	return rows
}

func (r *results) top() [][]string {
	type entry struct {
		word  string
		count int
	}
	entries := make([]entry, 0, len(r.frequency))
	for word, count := range r.frequency {
		entries = append(entries, entry{word, count})
	}
	slices.SortFunc(entries, func(a, b entry) int {
		if a.count != b.count {
			return b.count - a.count
		}
		return strings.Compare(a.word, b.word)
	})
	var rows [][]string
	for _, e := range entries[:min(topSize, len(entries))] {
		rows = append(rows, []string{e.word, strconv.Itoa(e.count)})
	}
	return rows
}

func (r *results) aboveAverage(average float64) [][]string {
	var rows [][]string
	for _, wc := range r.newCounts {
		if float64(wc.count) > average {
			rows = append(rows, []string{wc.url, strconv.Itoa(wc.count)})
		}
	}
	return rows
}

func parquetFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
			files = append(files, path)
		}
		return nil
	})
	slices.Sort(files)
	return files, err
}

func loadStopwords() (map[string]bool, error) {
	stopwords := map[string]bool{}
	for _, name := range []string{"stopwords/english", "stopwords/french"} {
		data, err := stopwordFiles.ReadFile(name)
		if err != nil {
			return nil, err
		}
		for _, word := range strings.Fields(string(data)) {
			stopwords[word] = true
		}
	}
	return stopwords, nil
}

func writeCSV(path string, rows [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	buffered := bufio.NewWriter(file)
	writer := csv.NewWriter(buffered)
	if err := writer.WriteAll(rows); err != nil {
		return err
	}
	return buffered.Flush()
}
