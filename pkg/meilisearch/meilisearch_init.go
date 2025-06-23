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

	client := &http.Client{}

	// 1. Check if index has primary key
	indexEndpoint := fmt.Sprintf("%s/indexes/%s", handler.BaseURL, handler.Index)
	req, err := http.NewRequest("GET", indexEndpoint, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %v", err)
	}
	MeilisearchHeader(req, handler.ApiKey)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	var meta struct {
		PrimaryKey *string `json:"primaryKey"`
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
			return fmt.Errorf("failed to decode index metadata: %v", err)
		}
	} else if resp.StatusCode == 404 {
		// Index does not exist yet
		meta.PrimaryKey = nil
	} else {
		return fmt.Errorf("failed to get index info: status %v", resp.Status)
	}

	// 2. If primary key is not set, set it
	if meta.PrimaryKey == nil {
		l.Println("Setting primary key on index before adding documents")
		settings := map[string]string{
			"primaryKey": "research_paper_id",
		}
		settingsBody, err := json.Marshal(settings)
		if err != nil {
			return fmt.Errorf("failed to marshal index settings: %v", err)
		}

		reqSettings, err := http.NewRequest("PATCH", indexEndpoint, bytes.NewBuffer(settingsBody))
		if err != nil {
			return fmt.Errorf("failed to create index settings request: %v", err)
		}
		reqSettings.Header.Set("Content-Type", "application/json")
		MeilisearchHeader(reqSettings, handler.ApiKey)

		respSettings, err := client.Do(reqSettings)
		if err != nil {
			return fmt.Errorf("failed to update index settings: %v", err)
		}
		defer respSettings.Body.Close()

		if respSettings.StatusCode < 200 || respSettings.StatusCode >= 300 {
			bodyBytes, _ := io.ReadAll(respSettings.Body)
			return fmt.Errorf("failed to set primary key: status %v, body: %s", respSettings.Status, string(bodyBytes))
		}

		l.Println("Primary key set successfully")
	} else {
		l.Printf("Primary key already set: %s\n", *meta.PrimaryKey)
	}

	for i, doc := range documents {
		id, ok := doc["research_paper_id"]
		if !ok {
			l.Printf("Document at index %d missing research_paper_id", i)
			continue
		}
		doc["uid"] = id
	}

	// 3. Marshal documents
	body, err := json.Marshal(documents)
	if err != nil {
		return fmt.Errorf("failed to marshal documents: %v", err)
	}

	// Optional: pretty-print JSON
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, body, "", "  "); err != nil {
		return fmt.Errorf("failed to format JSON body: %v", err)
	}
	fmt.Println("Formatted JSON body:\n", prettyJSON.String())

	// 4. Add documents
	documentsEndpoint := fmt.Sprintf("%s/indexes/%s/documents", handler.BaseURL, handler.Index)
	reqAddDocs, err := http.NewRequest("POST", documentsEndpoint, bytes.NewBuffer(prettyJSON.Bytes()))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request to add documents: %v", err)
	}
	reqAddDocs.Header.Set("Content-Type", "application/json")
	MeilisearchHeader(reqAddDocs, handler.ApiKey)

	respAddDocs, err := client.Do(reqAddDocs)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request to add documents: %v", err)
	}
	defer respAddDocs.Body.Close()

	bodyBytes, err := io.ReadAll(respAddDocs.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	l.Printf("Response status from Meilisearch: %v", respAddDocs.Status)
	l.Printf("Response body from Meilisearch: %s", string(bodyBytes))

	if respAddDocs.StatusCode >= 200 && respAddDocs.StatusCode < 300 {
		l.Print("Successfully initialized data in Meilisearch")
	} else {
		return fmt.Errorf("failed to initialize data in Meilisearch: %v", respAddDocs.Status)
	}

	return nil
}

