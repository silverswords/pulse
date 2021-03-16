package webhook

import (
	"net/http"

	"github.com/silverswords/pulse/pkg/topic"
)

// WithRequiredACK would turn on the ack function.
func WithWebHook() topic.Option {
	return func(t *topic.BundleTopic) error {
		panic("not implemented yet")
		return nil
	}
}

type webhookServer struct {
}

func (s *webhookServer) Add(pattern string, handler http.Handler) {
}

func NewHookServer() *webhookServer {
	return &webhookServer{}
}

// nolint
type webhookService struct {
}

func (s *webhookService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// do serve
}
