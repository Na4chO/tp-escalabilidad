package ArticleHandler

import "github.com/Na4chO/tp-escalabilidad/common/middleware"

var nextClientID uint64 = 0

type ArticleHandler struct {
	clientID uint64
}

func NewArticleHandler() ArticleHandler {
	clientID := nextClientID
	nextClientID++
	return ArticleHandler{clientID: clientID}
}

func (articleHandler *ArticleHandler) SerializeArticleMessage(raw string) (*middleware.Message, error) {
	return nil, nil
}

func (articleHandler *ArticleHandler) SerializeEofMessage(raw string) (*middleware.Message, error) {
	return nil, nil
}

func (articleHandler *ArticleHandler) DeserializeResultMessage(message *middleware.Message) (string, error) {
	return "", nil
}
