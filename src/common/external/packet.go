package external

import (
	"encoding/json"
	"fmt"

	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
)

type Packet[T any] struct {
	ClientID clientid.ID `json:"client_id"`
	IsEof    bool        `json:"is_eof"`
	Total    uint32      `json:"total"`
	Count    uint32      `json:"count"`
	Items    []T         `json:"items,omitempty"`
	Payload  T           `json:"payload,omitzero"`
}

func (e Packet[T]) Encode() ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encoding packet: %w", err)
	}
	return data, nil
}

func DecodePacket[T any](data []byte) (Packet[T], error) {
	var packet Packet[T]
	if err := json.Unmarshal(data, &packet); err != nil {
		return Packet[T]{}, fmt.Errorf("decoding packet: %w", err)
	}
	if int(packet.Count) != len(packet.Items) {
		return Packet[T]{}, fmt.Errorf("decoding packet: header says %d items but the batch has %d", packet.Count, len(packet.Items))
	}

	return packet, nil
}
