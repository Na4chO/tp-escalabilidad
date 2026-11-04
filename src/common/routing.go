package common

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
)

func PartitionKey(prefix string, partition int) string {
	return fmt.Sprintf("%s_%d", prefix, partition)
}

func ClientPartition(clientID clientid.ID, partitions int) int {
	if partitions < 1 {
		return 0
	}

	h := fnv.New64a()
	writeClientID(h, clientID)
	_, _ = h.Write([]byte{0})
	return int(h.Sum64() % uint64(partitions))
}

func ClientWordPartition(clientID clientid.ID, word string, partitions int) int {
	if partitions < 1 {
		return 0
	}

	h := fnv.New64a()
	writeClientID(h, clientID)
	_, _ = h.Write([]byte{1})
	_, _ = h.Write([]byte(word))
	return int(h.Sum64() % uint64(partitions))
}

func writeClientID(h interface{ Write([]byte) (int, error) }, clientID clientid.ID) {
	_, _ = h.Write([]byte(clientID.Server))
	_, _ = h.Write([]byte{0})

	var seq [4]byte
	binary.BigEndian.PutUint32(seq[:], clientID.Seq)
	_, _ = h.Write(seq[:])
}
