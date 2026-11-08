package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Wikimedia rate limits user agents without contact info
const userAgent = "SistemasDistribuidos-Grupo29/1.0 (https://github.com/Na4chO/tp-escalabilidad)"

var httpClient = &http.Client{Timeout: 30 * time.Second}

// The cache is shared by every client, IMAGE_CACHE puts it outside the data dir of each one
func imageCachePath(dataDir string) string {
	if path := os.Getenv("IMAGE_CACHE"); path != "" {
		return path
	}
	return filepath.Join(dataDir, "image_status.json")
}

func loadImageCache(path string) map[string]bool {
	cache := map[string]bool{}
	data, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(data, &cache)
	}
	return cache
}

// saveImageCache writes a temp file and renames it, so a reader never sees it half written
func saveImageCache(path string, cache map[string]bool) error {
	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	temp := fmt.Sprintf("%s.%d", path, os.Getpid())
	if err := os.WriteFile(temp, data, 0o644); err != nil {
		return nil // data/ may be read only, the cache is just an optimization
	}
	return os.Rename(temp, path)
}

// imageExists tells whether the url still serves an image. The dataset has urls that no longer
// exist (renamed or deleted files) and the system skips those articles. Only definitive answers are cached
func imageExists(url string, cache map[string]bool) bool {
	if exists, ok := cache[url]; ok {
		return exists
	}
	const attempts = 6
	for attempt := range attempts {
		wait, err := headImage(url)
		if err == nil {
			cache[url] = true
			return true
		}
		var gone errNotFound
		if errors.As(err, &gone) {
			cache[url] = false
			return false
		}
		if attempt < attempts-1 {
			time.Sleep(min(wait, time.Minute))
		}
	}
	fmt.Printf("  query2: could not check %s, assuming it exists\n", url)
	return true
}

type errNotFound struct{ status int }

func (e errNotFound) Error() string { return fmt.Sprintf("status %d", e.status) }

// headImage returns how long to wait before retrying when the request can be retried
func headImage(url string) (time.Duration, error) {
	request, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return 0, errNotFound{}
	}
	request.Header.Set("User-Agent", userAgent)
	response, err := httpClient.Do(request)
	if err != nil {
		return 2 * time.Second, err
	}
	response.Body.Close()

	switch {
	case response.StatusCode == http.StatusOK:
		return 0, nil
	case response.StatusCode == http.StatusTooManyRequests:
		seconds, _ := strconv.Atoi(response.Header.Get("Retry-After"))
		return max(time.Duration(seconds)*time.Second, 2*time.Second), fmt.Errorf("rate limited")
	default:
		return 0, errNotFound{status: response.StatusCode}
	}
}
