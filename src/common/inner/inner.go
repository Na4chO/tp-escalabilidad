package inner

const (
	QueryMetadata             uint8 = 1
	QueryImage                uint8 = 2
	QueryIntersections        uint8 = 3
	QueryTopWords             uint8 = 4
	QueryAbstractAboveAverage uint8 = 5
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
	Url   string `json:"url"`
	Count uint32 `json:"count"`
}

type PeerCount struct {
	Instance string `json:"instance"`
	Count    uint32 `json:"count"`
	Finished bool   `json:"finished"`
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
	DateModified      string      `json:"dateModified,omitempty"`
	Name              string      `json:"name,omitempty"`
	Image             []byte      `json:"image,omitempty"`
}

type ImageCandidateMetadata struct {
	Url      string `json:"url"`
	Name     string `json:"name"`
	ImageUrl string `json:"imageUrl"`
}
