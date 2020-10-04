package webhook

import "net/http"

type webhookServer struct {

}
func (s *webhookServer) Add(pattern string, handler http.Handler) {

}

func NewHookServer() *webhookServer {
	return &webhookServer{}
}

type webhookService struct{

}

func (s *webhookService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// do serve
}