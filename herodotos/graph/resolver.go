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
	form := &model.Form{}
	if v.Form != nil {
		form = &model.Form{ID: v.Form.Id, Blob: v.Form.Blob}
	}
	return &model.ReadingSession{ID: v.Id, UserID: v.UserId, FormID: v.FormId, Form: form, ProgressBlob: v.ProgressBlob, CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt}
}
