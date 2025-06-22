package meilisearch

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

func InitializeMeilisearchData(db *sql.DB, handler *MeiliSearchHandler, l *log.Logger) error {
	documents, err := handler.fetchDataFromDatabase(db)
	if err != nil {
		return fmt.Errorf("failed to fetch data from PostgreSQL: %v", err)
	}

	endpoint := fmt.Sprintf("%s/indexes/%s/documents", handler.BaseURL, handler.Index)
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}
	MeilisearchHeader(req, handler.ApiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		l.Println("Data already exists in Meilisearch")
		return nil
	}

	body, err := json.Marshal(documents)
	if err != nil {
		return fmt.Errorf("failed to marshal documents: %v", err)
	}

	req, err = http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}

	MeilisearchHeader(req, handler.ApiKey)
	resp, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		l.Print("Successfully initialized data in Meilisearch")
	} else {
		responseBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to initialize data in Meilisearch: %v, response: %s", resp.Status, string(responseBody))
	}

	return nil
}
