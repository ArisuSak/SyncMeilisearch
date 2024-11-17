package meilisearch

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"nats-jetstream/postgres"
	"nats-jetstream/utils"
	"net/http"

	"go.uber.org/zap"
)

const (
    TableName = "main.tenants"
	Document = "tenant"
    Host = "localhost"
    Port = 7700            
	Key  = "my-key"            
)

var meili_url = fmt.Sprintf("http://%s:%d", Host, Port)

func SendToMeilisearch(data []byte, l *log.Logger) error {
    var walData postgres.WALData
    err := json.Unmarshal(data, &walData)
    if err != nil {
        l.Fatal("Invalid JSON format", zap.Error(err))
        return err
    }

    l.Println("WAL Data", zap.Any("walData", walData))

    for _, change := range walData.Change {
        var endpoint string
        var method string
        var payload []byte

        //change to switch 
        if change.Kind == "insert" || change.Kind == "update" {
			meilisearchPayload, err:= prepareMeilisearchPayload[interface{}](change)
            if err != nil {
                l.Fatal("Failed to prepare Meilisearch payload", zap.Error(err))
				return err
            }
            payload = meilisearchPayload
       
            method = "POST"
            endpoint = fmt.Sprintf("%s/indexes/%s/documents", meili_url, Document)
        } else if change.Kind == "delete" {
            id, err := extractIDFromChange[interface{}](change)
            if err != nil {
                l.Fatal("Failed to extract ID for delete", zap.Error(err))
                continue
            }
            endpoint = fmt.Sprintf("%s/indexes/%s/documents/%s", meili_url, Document, id)
            method = "DELETE"
        } else {
            l.Println("Unknown WAL operation kind", zap.String("kind", change.Kind))
            continue
        }

        req, err := http.NewRequest(method, endpoint, bytes.NewBuffer(payload))
        if err != nil {
            l.Fatal("Failed to create HTTP request", zap.Error(err))
            continue
        }

        utils.MeilisearchHeader(req, Key)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err != nil {
            l.Fatal("Failed to send HTTP request", zap.Error(err))
            continue
        }
        defer resp.Body.Close()

        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            l.Println("Successfully sent data to Meilisearch", zap.String("operation", change.Kind), zap.Int("status", resp.StatusCode))
        } else {
            l.Fatal("Failed to send data to Meilisearch", zap.Int("status", resp.StatusCode))
        }
    }

    return nil
}

func InitializeMeilisearchData(db *sql.DB, l *zap.Logger) error {
    documents, err := fetchDataFromDatabase(db)
    if err != nil {
        return fmt.Errorf("failed to fetch data from PostgreSQL: %v", err)
    }

    endpoint := fmt.Sprintf("%s/indexes/%s/documents", meili_url, Document)
    req, err := http.NewRequest("GET", endpoint, nil)
    if err != nil {
        return fmt.Errorf("failed to create HTTP request: %v", err)
    }
    req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", Key))

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return fmt.Errorf("failed to send HTTP request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusOK {
        l.Info("Data already exists in Meilisearch")
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
    utils.MeilisearchHeader(req, Key)

    resp, err = client.Do(req)
    if err != nil {
        return fmt.Errorf("failed to send HTTP request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 200 && resp.StatusCode < 300 {
        l.Info("Successfully initialized data in Meilisearch")
    } else {
        responseBody, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("failed to initialize data in Meilisearch: %v, response: %s", resp.Status, string(responseBody))
    }

    return nil
}
