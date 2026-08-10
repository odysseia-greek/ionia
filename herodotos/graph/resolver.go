package graph

import (
	diodorosv1 "github.com/odysseia-greek/ionia/diodoros/gen/go/v1"
	"github.com/odysseia-greek/ionia/herodotos/graph/model"
	thoukydidesv1 "github.com/odysseia-greek/ionia/thoukydides/gen/go/v1"
)

type Resolver struct {
	Corpus diodorosv1.DiodorosServiceClient
	Core   thoukydidesv1.ThoukydidesServiceClient
}

func value(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func reading(v *thoukydidesv1.ReadingSession) *model.ReadingSession {
	return &model.ReadingSession{ID: v.Id, UserID: v.UserId, Author: v.Author, Book: v.Book, Reference: v.Reference, Section: v.Section, CreatedAt: v.CreatedAt}
}
