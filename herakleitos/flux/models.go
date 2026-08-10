package flux

type Text struct {
	Author          string `json:"author"`
	Book            string `json:"book"`
	Type            string `json:"type"`
	Reference       string `json:"reference"`
	PerseusTextLink string `json:"perseusTextLink"`
	Rhemai          []struct {
		Greek        string   `json:"greek"`
		Translations []string `json:"translations"`
		Section      string   `json:"section"`
	} `json:"rhemai"`
}
