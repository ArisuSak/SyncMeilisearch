package meilisearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"nats-jetstream/pkg/postgres"
	"net/http"
)

func (m *MeiliSearchHandler) ProcessWalData(data []byte, l *log.Logger) error {

	l.Printf("WAL data In process: %s", string(data))
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

	processor := DefaultMeilisearchProcessor[string]{
		PrimaryKey: m.PK,
	}

	changeJSON, _ := json.Marshal(change)
	fmt.Println("orginal change:", string(changeJSON))
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

	log.Printf("Sending %s request to %s with payload: %s", method, endpoint, string(payload))

	req, err := http.NewRequest(method, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	MeilisearchHeader(req, m.ApiKey)

	client := &http.Client{}
	resp, err := client.Do(req)
	
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("request failed with status %s", resp.Status)
	}

	log.Println("Successfully synced data with Meilisearch:", string(payload))
	return nil
}
