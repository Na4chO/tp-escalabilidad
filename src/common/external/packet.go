package external

import (
	"encoding/json"
	"fmt"
)

type Packet[T any] struct {
	ClientID uint32 `json:"client_id"`
	IsEof    bool   `json:"is_eof"`
	Payload  T      `json:"payload,omitzero"`
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

	return packet, nil
}
