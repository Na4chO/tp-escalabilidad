package preprocessing

import "strings"

const englishStopWords = `
a about above after again against ain all am an and any are aren aren't as at be because
been before being below between both but by can couldn couldn't d did didn didn't do does
doesn doesn't doing don don't down during each few for from further had hadn hadn't has
hasn hasn't have haven haven't having he he'd he'll her here hers herself he's him himself
his how i i'd if i'll i'm in into is isn isn't it it'd it'll it's its itself i've just ll
m ma me mightn mightn't more most mustn mustn't my myself needn needn't no nor not now o
of off on once only or other our ours ourselves out over own re s same shan shan't she
she'd she'll she's should shouldn shouldn't should've so some such t than that that'll
the their theirs them themselves then there these they they'd they'll they're they've this
those through to too under until up ve very was wasn wasn't we we'd we'll we're were weren
weren't we've what when where which while who whom why will with won won't wouldn wouldn't
y you you'd you'll your you're yours yourself yourselves you've
`

const frenchStopWords = `
au aux avec ce ces dans de des du elle en et eux il ils je la le les leur lui ma mais me
même mes moi mon ne nos notre nous on ou par pas pour qu que qui sa se ses son sur ta te
tes toi ton tu un une vos votre vous c d j l à m n s t y été étée étées étés étant étante
étants étantes suis es est sommes êtes sont serai seras sera serons serez seront serais
serait serions seriez seraient étais était étions étiez étaient fus fut fûmes fûtes furent
sois soit soyons soyez soient fusse fusses fût fussions fussiez fussent ayant ayante ayantes
ayants eu eue eues eus ai as avons avez ont aurai auras aura aurons aurez auront aurais
aurait aurions auriez auraient avais avait avions aviez avaient eut eûmes eûtes eurent
aie aies ait ayons ayez aient eusse eusses eût eussions eussiez eussent
`

var StopWords = newStopWords(englishStopWords, frenchStopWords)

func newStopWords(lists ...string) map[string]struct{} {
	words := make(map[string]struct{})
	for _, list := range lists {
		for _, word := range strings.Fields(list) {
			words[strings.ToLower(word)] = struct{}{}
		}
	}
	return words
}

func IsStopWord(word string) bool {
	_, found := StopWords[strings.ToLower(word)]
	return found
}
