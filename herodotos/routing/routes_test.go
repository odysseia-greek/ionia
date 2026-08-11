package routing

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/odysseia-greek/ionia/herodotos/gateway"
)

func TestHealthRoutes(t *testing.T) {
	router := InitRoutes(&gateway.HerodotosHandler{})
	for _, path := range []string{"/healthz", "/readyz"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s returned %d", path, response.Code)
		}
		if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
			t.Fatalf("%s returned content type %q", path, contentType)
		}
	}
}
