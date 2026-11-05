package external

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/Na4chO/tp-escalabilidad/common/article"
	"github.com/Na4chO/tp-escalabilidad/common/external/safeio"
	"github.com/Na4chO/tp-escalabilidad/common/external/serializer"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

type MsgType uint8

const (
	Article MsgType = iota + 1
	EndOfFile
	QueryResult
)

func WriteArticles(writer io.Writer, articles []article.Article) error {
	message := []byte{byte(Article)}
	message = append(message, serializer.SerializeUint32(uint32(len(articles)))...)
	for _, a := range articles {
		data, err := json.Marshal(a)
		if err != nil {
			return fmt.Errorf("encoding article: %w", err)
		}
		message = append(message, serializer.SerializeUint32(uint32(len(data)))...)
		message = append(message, data...)
	}
	if err := safeio.WriteAll(writer, message); err != nil {
		return fmt.Errorf("writing articles: %w", err)
	}
	return nil
}

func ReadArticles[T any](reader io.Reader) ([]T, error) {
	header, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, fmt.Errorf("reading articles count: %w", err)
	}
	count := serializer.DeserializeUint32(header)
	articles := make([]T, 0, count)
	for range count {
		var a T
		if err := readBody(reader, &a); err != nil {
			return nil, err
		}
		articles = append(articles, a)
	}
	return articles, nil
}

func WriteEndOfFile(writer io.Writer) error {
	return writeMsgType(writer, EndOfFile)
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	data, err := safeio.ReadAll(reader, 1)
	if err != nil {
		return 0, fmt.Errorf("reading message type: %w", err)
	}
	return MsgType(data[0]), nil
}

func writeMsgType(writer io.Writer, msgType MsgType) error {
	if err := safeio.WriteAll(writer, []byte{byte(msgType)}); err != nil {
		return fmt.Errorf("writing message type: %w", err)
	}
	return nil
}

func writeMessage(writer io.Writer, msgType MsgType, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encoding message: %w", err)
	}
	if err := writeMsgType(writer, msgType); err != nil {
		return err
	}
	return WriteFrame(writer, data)
}

func readBody(reader io.Reader, body any) error {
	data, err := ReadFrame(reader)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, body); err != nil {
		return fmt.Errorf("decoding message: %w", err)
	}
	return nil
}

func WriteQueryResult(writer io.Writer, result inner.QueryResult) error {
	return writeMessage(writer, QueryResult, result)
}

func ReadQueryResult(reader io.Reader) (inner.QueryResult, error) {
	var result inner.QueryResult
	if err := readBody(reader, &result); err != nil {
		return inner.QueryResult{}, err
	}
	return result, nil
}
