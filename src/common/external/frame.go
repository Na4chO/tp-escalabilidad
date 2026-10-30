package external

import (
	"fmt"
	"io"

	"github.com/Na4chO/tp-escalabilidad/common/external/safeio"
	"github.com/Na4chO/tp-escalabilidad/common/external/serializer"
)

func WritePacket[T any](writer io.Writer, packet Packet[T]) error {
	data, err := packet.Encode()
	if err != nil {
		return err
	}
	return WriteFrame(writer, data)
}

func ReadPacket[T any](reader io.Reader) (Packet[T], error) {
	data, err := ReadFrame(reader)
	if err != nil {
		return Packet[T]{}, err
	}
	return DecodePacket[T](data)
}

func WriteFrame(writer io.Writer, data []byte) error {
	frame := serializer.SerializeUint32(uint32(len(data)))
	frame = append(frame, data...)
	if err := safeio.WriteAll(writer, frame); err != nil {
		return fmt.Errorf("writing frame: %w", err)
	}
	return nil
}

func ReadFrame(reader io.Reader) ([]byte, error) {
	header, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, fmt.Errorf("reading frame header: %w", err)
	}

	size := serializer.DeserializeUint32(header)
	data, err := safeio.ReadAll(reader, size)
	if err != nil {
		return nil, fmt.Errorf("reading frame body: %w", err)
	}
	return data, nil
}
