package safeio

import "io"

func ReadAll(reader io.Reader, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	nread, err := io.ReadFull(reader, buf)
	if err != nil || uint32(nread) < size {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func WriteAll(writer io.Writer, msg []byte) error {
	written := 0
	for written < len(msg) {
		nwritten, err := writer.Write(msg[written:])
		if err != nil {
			return err
		}
		written += nwritten
	}
	return nil
}
