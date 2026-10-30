package inner

type Article struct {
	Abstract           string `json:"abstract"`
	AdditionalEntities string `json:"additionalEntities"`
	DateCreated        string `json:"dateCreated"`
	DateModified       string `json:"dateModified"`
	Description        string `json:"description"`
	Event              string `json:"event"`
	Identifier         uint32 `json:"identifier"`
	Image              string `json:"image"`
	InLanguage         string `json:"inLanguage"`
	Infoboxes          string `json:"infoboxes"`
	IsPartOf           string `json:"isPartOf"`
	License            string `json:"license"`
	MainEntity         string `json:"mainEntity"`
	Name               string `json:"name"`
	References         string `json:"references"`
	Sections           string `json:"sections"`
	Tables             string `json:"tables"`
	Url                string `json:"url"`
	Version            string `json:"version"`
}

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
