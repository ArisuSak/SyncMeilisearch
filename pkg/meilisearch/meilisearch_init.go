package meilisearch

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	meili "github.com/meilisearch/meilisearch-go"
)

func (m *MeiliSearchHandler) CreateIndex(
	meiliClient meili.ServiceManager,
	l *log.Logger,
	index string,
	pk string,
) error {
	// Try to get the existing index
	existingIndex, err := meiliClient.GetIndex(index)
	if err != nil {
		if apiErr, ok := err.(*meili.Error); ok && apiErr.MeilisearchApiError.Code == "index_not_found" {
			_, err := meiliClient.CreateIndex(&meili.IndexConfig{
				Uid:        index,
				PrimaryKey: pk,
			})
			if err != nil {
				return fmt.Errorf("failed to create index: %v", err)
			}
			l.Printf("Created index %q with primary key %q", index, pk)
			return nil
		}
		return fmt.Errorf("failed to get index: %v", err)
	}

	// Index exists, check primary key
	if existingIndex.PrimaryKey == "" {
		l.Printf("Index %q exists but has no primary key. Deleting and recreating with primary key %q", index, pk)

		_, err := meiliClient.DeleteIndex(index)
		if err != nil {
			return fmt.Errorf("failed to delete index: %v", err)
		}

		_, err = meiliClient.CreateIndex(&meili.IndexConfig{
			Uid:        index,
			PrimaryKey: pk,
		})
		if err != nil {
			return fmt.Errorf("failed to recreate index: %v", err)
		}

		l.Printf("Recreated index %q with primary key %q", index, pk)
	} else {
		l.Printf("Index %q already exists with primary key %q", index, existingIndex.PrimaryKey)
	}

	return nil
}


func InitializeMeilisearchDataByClient(
	db *sql.DB,
	handler *MeiliSearchHandler,
	meiliClient meili.ServiceManager,
	l *log.Logger,
	index string ,
	pk string,
) error {
	documents, err := handler.fetchDataFromDatabase(db)
	if err != nil {
		return fmt.Errorf("failed to fetch data from PostgreSQL: %v", err)
	}

	_, err = meiliClient.Index(index).AddDocuments(documents, pk)
	if err != nil {
		return fmt.Errorf("failed to add documents to Meilisearch: %v", err)
	}

	l.Printf("Successfully initialized Meilisearch index '%s' with %d documents", index, len(documents))
	return nil
}

func InitializeMeilisearchData(db *sql.DB, handler *MeiliSearchHandler, meiliClient meili.ServiceManager, l *log.Logger, index string, pk string) error {
	documents, err := handler.fetchDataFromDatabase(db)
	if err != nil {
		return fmt.Errorf("failed to fetch data from PostgreSQL: %v", err)
	}

	client := &http.Client{}

	for i, doc := range documents {
		id, ok := doc[pk]
		if !ok {
			l.Printf("Document at index %d missing %s", i, pk)
			continue
		}
		doc["uid"] = id
		doc["PrimaryKey"] = id  
	}

	body, err := json.Marshal(documents)
	if err != nil {
		return fmt.Errorf("failed to marshal documents: %v", err)
	}


	// Add documents
	documentsEndpoint := fmt.Sprintf("%s/indexes/%s/documents?primaryKey", handler.BaseURL, handler.Index)
	// reqAddDocs, err := http.NewRequest("POST", documentsEndpoint, bytes.NewBuffer(prettyJSON.Bytes()))
	reqAddDocs, err := http.NewRequest("POST", documentsEndpoint, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request to add documents: %v", err)
	}

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

	l.Printf("Response body from Meilisearch: %s", string(bodyBytes))

	if respAddDocs.StatusCode >= 200 && respAddDocs.StatusCode < 300 {
		l.Print("Successfully initialized data in Meilisearch")
	} else {
		return fmt.Errorf("failed to initialize data in Meilisearch: %v", respAddDocs.Status)
	}

	return nil
}

