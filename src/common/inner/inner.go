package inner

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

type AbstractWordCount struct {
	Count      uint16 `json:"count"`
	InLanguage string `json:"inLanguage"`
}

type Average struct {
	Average float64 `json:"average"`
}

type QueryResult struct {
	QueryType        uint8             `json:"queryType"`
	IntersectionWord string            `json:"intersectionWord,omitempty"`
	TopWords         map[string]uint32 `json:"topWords,omitempty"`
	Url              string            `json:"url,omitempty"`
	WordCount        uint32            `json:"wordCount,omitempty"`
}
