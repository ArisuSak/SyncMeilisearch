package meilisearch

import "net/http"

func MeilisearchHeader(r *http.Request, key string) {
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("Content-Type", "application/json")
}
