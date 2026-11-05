package external

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Na4chO/tp-escalabilidad/common/article"
)

func TestArticleBatchesKeepOrder(t *testing.T) {
	var stream bytes.Buffer
	sent := 0
	for _, size := range []int{1, 7, 3, 100} {
		batch := make([]article.Article, size)
		for i := range batch {
			batch[i] = article.Article{Url: fmt.Sprintf("url-%d", sent), Abstract: "some words"}
			sent++
		}
		if err := WriteArticles(&stream, batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteEndOfFile(&stream); err != nil {
		t.Fatal(err)
	}

	received := 0
	for {
		msgType, err := ReadMsgType(&stream)
		if err != nil {
			t.Fatal(err)
		}
		if msgType == EndOfFile {
			break
		}
		articles, err := ReadArticles[article.Article](&stream)
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range articles {
			if want := fmt.Sprintf("url-%d", received); a.Url != want {
				t.Fatalf("got %s, want %s", a.Url, want)
			}
			received++
		}
	}
	if received != sent {
		t.Fatalf("received %d articles, sent %d", received, sent)
	}
	if stream.Len() != 0 {
		t.Fatalf("%d bytes left after the EOF", stream.Len())
	}
}
