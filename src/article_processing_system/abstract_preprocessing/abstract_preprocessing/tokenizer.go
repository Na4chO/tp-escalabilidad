package abstractpreprocessing

import (
	"regexp"
	"strings"
)

var (
	wordRegex   = regexp.MustCompile(`[\p{L}\p{N}_]+`)
	numberRegex = regexp.MustCompile(`^[+-]?(([0-9]+\.?[0-9]*)|(\.[0-9]+))$`)
)

func extractWords(abstract string) []string {
	matches := wordRegex.FindAllString(strings.ToLower(abstract), -1)
	words := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))

	for _, word := range matches {
		if IsStopWord(word) {
			continue
		}
		if numberRegex.MatchString(word) {
			continue
		}
		if _, isDuplicate := seen[word]; isDuplicate {
			continue
		}
		seen[word] = struct{}{}
		words = append(words, word)
	}

	return words
}
