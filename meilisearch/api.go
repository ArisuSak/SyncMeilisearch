package meilisearch

import (
	"bytes"
	"encoding/json"
	"fmt"
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

	l.Println("WAL Data:", walData)

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

		fmt.Println("Successfully prepared payload:", string(payload))

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

	fmt.Println("Payload right before sending:", string(payload))

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
