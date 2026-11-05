package articlehandler

import (
	"fmt"
	"strconv"

	"github.com/Na4chO/tp-escalabilidad/common"
	clientid "github.com/Na4chO/tp-escalabilidad/common/client_id"
	"github.com/Na4chO/tp-escalabilidad/common/inner"
)

type Article struct {
	Abstract     string `json:"abstract"`
	DateModified int64  `json:"dateModified"`
	Name         string `json:"name"`
	Url          string `json:"url"`
	Sections     string `json:"sections"`
	InLanguage   struct {
		Identifier string `json:"identifier"`
	} `json:"inLanguage"`
	Image struct {
		ContentUrl string `json:"content_url"`
	} `json:"image"`
}

type ArticleHandler struct {
	metadataQueue *common.PacketQueue[inner.Metadata]
	abstractQueue *common.PacketQueue[inner.Abstract]
}

func NewArticleHandler(metadataQueue *common.PacketQueue[inner.Metadata], abstractQueue *common.PacketQueue[inner.Abstract]) *ArticleHandler {
	return &ArticleHandler{metadataQueue, abstractQueue}
}

func (articleHandler *ArticleHandler) HandleArticle(clientID clientid.ID, a Article) error {
	dateModified := strconv.FormatInt(a.DateModified, 10)
	inLanguage := a.InLanguage.Identifier
	if inLanguage == "" {
		return fmt.Errorf("article %s has no language", a.Url)
	}
	imageURL := a.Image.ContentUrl

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

func (articleHandler *ArticleHandler) HandleEOF(clientID clientid.ID) error {
	if err := articleHandler.metadataQueue.SendEOF(clientID); err != nil {
		return err
	}
	return articleHandler.abstractQueue.SendEOF(clientID)
}
