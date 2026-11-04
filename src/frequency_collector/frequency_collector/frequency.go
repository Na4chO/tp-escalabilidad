package frequency_collector

import (
	"cmp"
	"slices"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/external"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

type idiomsFrequency struct {
	EnglishFrequency uint32
	FrenchFrequency  uint32
}

type clientState struct {
	wordsFreq map[string]*idiomsFrequency
}

func (fc *FrequencyCollector) updateFrequencies(clientID clientid.ID, abstractWords inner.AbstractWords) []string {
	var intersections []string

	fc.mu.Lock()
	defer fc.mu.Unlock()

	state := fc.clientStateForLocked(clientID)
	if abstractWords.InLanguage != "en" && abstractWords.InLanguage != "fr" {
		return intersections
	}

	for _, word := range abstractWords.Words {
		wordFreq, ok := state.wordsFreq[word]
		if !ok {
			wordFreq = &idiomsFrequency{}
			state.wordsFreq[word] = wordFreq
		}
		if isIntersection(abstractWords.InLanguage, wordFreq) {
			intersections = append(intersections, word)
		}

		if abstractWords.InLanguage == "en" {
			wordFreq.EnglishFrequency++
		} else {
			wordFreq.FrenchFrequency++
		}
	}

	return intersections
}

func isIntersection(inLanguage string, wordFreq *idiomsFrequency) bool {
	switch inLanguage {
	case "en":
		return wordFreq.EnglishFrequency == 0 && wordFreq.FrenchFrequency > 0
	case "fr":
		return wordFreq.FrenchFrequency == 0 && wordFreq.EnglishFrequency > 0
	default:
		return false
	}
}

func (fc *FrequencyCollector) sendIntersections(clientID clientid.ID, intersections []string) error {
	return fc.resultQueue.Send(external.Packet[inner.QueryResult]{
		ClientID: clientID,
		Payload: inner.QueryResult{
			QueryType:         inner.QueryIntersections,
			IntersectionWords: intersections,
		},
	})
}

func (fc *FrequencyCollector) clientStateForLocked(clientID clientid.ID) *clientState {
	state, ok := fc.clientStates[clientID]
	if !ok {
		state = &clientState{wordsFreq: make(map[string]*idiomsFrequency)}
		fc.clientStates[clientID] = state
	}
	return state
}

func (fc *FrequencyCollector) topWords(state *clientState) []inner.WordCount {
	top := make([]inner.WordCount, 0, len(state.wordsFreq))
	for word, freq := range state.wordsFreq {
		top = append(top, inner.WordCount{
			Word:  word,
			Count: freq.EnglishFrequency + freq.FrenchFrequency,
		})
	}

	slices.SortFunc(top, func(a, b inner.WordCount) int {
		if byCount := cmp.Compare(b.Count, a.Count); byCount != 0 {
			return byCount
		}
		return cmp.Compare(a.Word, b.Word)
	})

	if len(top) > fc.topSize {
		top = top[:fc.topSize]
	}

	return top
}
