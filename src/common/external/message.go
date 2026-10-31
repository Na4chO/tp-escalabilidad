package external

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/Na4chO/tp-escalabilidad/common/article"
	"github.com/Na4chO/tp-escalabilidad/common/external/safeio"
)

type MsgType uint8

const (
	Article MsgType = iota + 1
	EndOfFile
)

func WriteArticle(writer io.Writer, a article.Article) error {
	return writeMessage(writer, Article, a)
}

func ReadArticle(reader io.Reader) (article.Article, error) {
	var a article.Article
	if err := readBody(reader, &a); err != nil {
		return article.Article{}, err
	}
	return a, nil
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
