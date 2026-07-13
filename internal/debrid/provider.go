package debrid

import (
	"context"
	"errors"

	"github.com/LTAGROUP/watchtower/internal/model"
)

var ErrStaleItem = errors.New("debrid item is no longer available")

type Provider interface {
	Name() string
	Resolve(context.Context, model.Release) (model.Resolved, error)
	StreamURL(context.Context, *model.File) (string, error)
}
