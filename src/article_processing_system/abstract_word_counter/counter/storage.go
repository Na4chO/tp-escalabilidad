package counter

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
)

func countsPath(dir string, clientID clientid.ID) string {
	return filepath.Join(dir, clientID.String()+".txt")
}

func appendCount(dir string, clientID clientid.ID, url string, count uint32) error {
	file, err := os.OpenFile(countsPath(dir, clientID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%s:%d\n", url, count)
	return err
}

func scanCounts(dir string, clientID clientid.ID, handle func(url string, count uint32) error) error {
	file, err := os.Open(countsPath(dir, clientID))

	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		sep := strings.LastIndexByte(line, ':')

		if sep < 0 {

			return fmt.Errorf("malformed line %q", line)
		}

		count, err := strconv.ParseUint(line[sep+1:], 10, 32)

		if err != nil {
			return fmt.Errorf("malformed line %q: %w", line, err)
		}
		if err := handle(line[:sep], uint32(count)); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func removeCounts(dir string, clientID clientid.ID) error {
	err := os.Remove(countsPath(dir, clientID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
