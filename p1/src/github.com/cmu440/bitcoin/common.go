package bitcoin

import (
	"github.com/cmu440/lsp"
)

func MakeParams(epochLimit, epochMillis, windowSize int) *lsp.Params {
	return &lsp.Params{
		EpochLimit:  epochLimit,
		EpochMillis: epochMillis,
		WindowSize:  windowSize,
	}
}

type QueryWithMessage struct {
	QueryId int
	Message Message
}
