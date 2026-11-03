package inner

const (
	QueryIntersections uint8 = 3
	QueryTopWords      uint8 = 4
)

type Metadata struct {
	Url          string `json:"url"`
	DateModified string `json:"dateModified"`
	InLanguage   string `json:"inLanguage"`
	Name         string `json:"name"`
	ImageUrl     string `json:"imageUrl"`
	Sections     string `json:"sections"`
}

type Abstract struct {
	Abstract     string `json:"abstract"`
	Url          string `json:"url"`
	DateModified string `json:"dateModified"`
	InLanguage   string `json:"inLanguage"`
}

type AbstractWords struct {
	Words      []string `json:"words"`
	InLanguage string   `json:"inLanguage"`
}

type PartialTop struct {
	Top []WordCount `json:"words"`
}

type WordCount struct {
	Word  string `json:"word"`
	Count uint32 `json:"count"`
}

type AbstractWordCount struct {
	Count      uint16 `json:"count"`
	InLanguage string `json:"inLanguage"`
}

type Average struct {
	Average float64 `json:"average"`
}

type QueryResult struct {
	QueryType         uint8       `json:"queryType"`
	IntersectionWords []string    `json:"intersectionWords,omitempty"`
	TopWords          []WordCount `json:"topWords,omitempty"`
	Url               string      `json:"url,omitempty"`
	WordCount         uint32      `json:"wordCount,omitempty"`
}
