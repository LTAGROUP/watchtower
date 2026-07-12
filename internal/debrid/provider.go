package debrid

import (
	"context"
	"github.com/LTAGROUP/watchtower/internal/model"
)

type Provider interface {
	Name() string
	Resolve(context.Context, model.Release) (model.Resolved, error)
	StreamURL(context.Context, *model.File) (string, error)
}
