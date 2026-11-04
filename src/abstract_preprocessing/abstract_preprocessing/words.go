package abstract_preprocessing

import (
	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
)

func groupWordsByPartition(clientID clientid.ID, words []string, partitions int) map[int][]string {
	grouped := make(map[int][]string)
	for _, word := range words {
		partition := common.ClientWordPartition(clientID, word, partitions)
		grouped[partition] = append(grouped[partition], word)
	}
	return grouped
}
