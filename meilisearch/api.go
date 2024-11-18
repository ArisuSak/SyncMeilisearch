package meilisearch

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"nats-jetstream/postgres"
	"net/http"
)

func (m *MeiliSearchHandler) ProcessWalData(data []byte, l *log.Logger) error {
	var walData postgres.WALData
	err := json.Unmarshal(data, &walData)
	if err != nil {
		l.Printf("Error unmarshalling WAL data: %v", err)
		return err
	}

	for _, change := range walData.Change {
		if err := m.ProcessChange(change); err != nil {
			l.Printf("Error processing change: %v", err)
			return err
		}
	}

	return nil
}

func (m *MeiliSearchHandler) ProcessChange(change postgres.WALChange) error {
	var endpoint, method string
	var payload []byte
	processor := DefaultMeilisearchProcessor[string]{}

	switch change.Kind {
	case "insert", "update":
		preparePayload, err := processor.preparePayload(change)

		if err != nil {
			return fmt.Errorf("failed to prepare payload: %w", err)
		}
		payload = preparePayload

		method = "POST"
		endpoint = fmt.Sprintf("%s/indexes/%s/documents", m.BaseURL, m.Index)
	case "delete":
		id, err := processor.extractIDFromChange(change)
		if err != nil {
			return fmt.Errorf("failed to extract ID: %w", err)
		}
		method = "DELETE"
		endpoint = fmt.Sprintf("%s/indexes/%s/documents/%s", m.BaseURL, m.Index, id)
	default:
		return fmt.Errorf("unknown change kind: %s", change.Kind)
	}

	return m.sendHTTPRequest(method, endpoint, payload)
}

func (m *MeiliSearchHandler) sendHTTPRequest(method, endpoint string, payload []byte) error {
	req, err := http.NewRequest(method, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", m.ApiKey))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request failed with status %s", resp.Status)
	}

	return nil
}

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
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", handler.ApiKey))

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
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", handler.ApiKey))

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
