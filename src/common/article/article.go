package article

type Article struct {
	Abstract           string         `json:"abstract"`
	AdditionalEntities []any          `json:"additionalEntities"`
	DateCreated        int64          `json:"dateCreated"`
	DateModified       int64          `json:"dateModified"`
	Description        string         `json:"description"`
	Event              map[string]any `json:"event"`
	Identifier         int64          `json:"identifier"`
	Image              map[string]any `json:"image"`
	InLanguage         map[string]any `json:"inLanguage"`
	Infoboxes          string         `json:"infoboxes"`
	IsPartOf           map[string]any `json:"isPartOf"`
	License            []any          `json:"license"`
	MainEntity         map[string]any `json:"mainEntity"`
	Name               string         `json:"name"`
	References         []any          `json:"references"`
	Sections           string         `json:"sections"`
	Tables             string         `json:"tables"`
	Url                string         `json:"url"`
	Version            map[string]any `json:"version"`
}

func FromRow(row map[string]any) Article {
	return Article{
		Abstract:           get[string](row, "abstract"),
		AdditionalEntities: get[[]any](row, "additional_entities"),
		DateCreated:        get[int64](row, "date_created"),
		DateModified:       get[int64](row, "date_modified"),
		Description:        get[string](row, "description"),
		Event:              get[map[string]any](row, "event"),
		Identifier:         get[int64](row, "identifier"),
		Image:              get[map[string]any](row, "image"),
		InLanguage:         get[map[string]any](row, "in_language"),
		Infoboxes:          get[string](row, "infoboxes"),
		IsPartOf:           get[map[string]any](row, "is_part_of"),
		License:            get[[]any](row, "license"),
		MainEntity:         get[map[string]any](row, "main_entity"),
		Name:               get[string](row, "name"),
		References:         get[[]any](row, "references"),
		Sections:           get[string](row, "sections"),
		Tables:             get[string](row, "tables"),
		Url:                get[string](row, "url"),
		Version:            get[map[string]any](row, "version"),
	}
}

func get[T any](row map[string]any, key string) T {
	value, _ := row[key].(T)
	return value
}
