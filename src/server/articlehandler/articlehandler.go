package ArticleHandler

import (
	"fmt"
	"strconv"

	"github.com/Na4chO/tp-escalabilidad/common"
	"github.com/Na4chO/tp-escalabilidad/common/article"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

type ArticleHandler struct {
	metadataQueue *common.PacketQueue[inner.Metadata]
	abstractQueue *common.PacketQueue[inner.Abstract]
}

func NewArticleHandler(metadataQueue *common.PacketQueue[inner.Metadata], abstractQueue *common.PacketQueue[inner.Abstract]) *ArticleHandler {
	return &ArticleHandler{metadataQueue, abstractQueue}
}

func (articleHandler *ArticleHandler) HandleArticle(clientID uint32, a article.Article) error {
	dateModified := strconv.FormatInt(a.DateModified, 10)
	inLanguage, ok := getLang(a.InLanguage)
	if !ok {
		return fmt.Errorf("language %s not recognized", a.InLanguage)
	}
	imageURL := getImg(a.Image)

	metadata := inner.Metadata{
		Url:          a.Url,
		DateModified: dateModified,
		InLanguage:   inLanguage,
		Name:         a.Name,
		ImageUrl:     imageURL,
		Sections:     a.Sections,
	}
	abstract := inner.Abstract{
		Abstract:     a.Abstract,
		Url:          a.Url,
		DateModified: dateModified,
		InLanguage:   inLanguage,
	}
	if err := articleHandler.metadataQueue.SendData(clientID, metadata); err != nil {
		return err
	}
	return articleHandler.abstractQueue.SendData(clientID, abstract)
}

func (articleHandler *ArticleHandler) HandleEOF(clientID uint32) error {
	if err := articleHandler.metadataQueue.SendEOF(clientID); err != nil {
		return err
	}
	return articleHandler.abstractQueue.SendEOF(clientID)
}

func getLang(value map[string]any) (string, bool) {
	identifier, ok := value["identifier"].(string)
	if !ok || identifier == "" {
		return "", false
	}
	return identifier, true
}

func getImg(value map[string]any) string {
	if url, ok := value["content_url"].(string); ok {
		return url
	}
	return ""
}
