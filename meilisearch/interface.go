package meilisearch

import (
	"database/sql"
	"log"

	"go.uber.org/zap"
)

type WalProcessor interface {
	ProcessWalData(data []byte, l *log.Logger) error
}

type DatabaseInitializer interface {
	InitializeData(db *sql.DB, l *zap.Logger) error
}

type MeiliSearchHandler struct {
	BaseURL   string
	ApiKey    string
	TableName string
	Index     string
}

func NewMeiliSearchHandler(baseURL, apiKey string, tableName string, index string) *MeiliSearchHandler {
	return &MeiliSearchHandler{
		BaseURL:   baseURL,
		ApiKey:    apiKey,
		TableName: tableName,
		Index:     index,
	}
}

func (m *MeiliSearchHandler) HandleMessage(data []byte, logger *log.Logger) error {
	err := m.ProcessWalData(data, logger)
	if err != nil {
		logger.Printf("Error processing WAL data: %v", err)
		return err
	}
	return nil
}
