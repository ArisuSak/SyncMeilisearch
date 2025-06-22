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
	BaseURL        string
	ApiKey         string
	TableName      string
	Index          string
	EnableInitData bool
	DB             *sql.DB
}

func NewMeiliSearchHandler(db *sql.DB, baseURL, apiKey, tableName, index string, enableInitData bool, logger *log.Logger) (*MeiliSearchHandler, error) {
	handler := &MeiliSearchHandler{
		BaseURL:        baseURL,
		ApiKey:         apiKey,
		TableName:      tableName,
		Index:          index,
		EnableInitData: enableInitData,
		DB:             db,
	}

	if enableInitData {
		logger.Println("Auto-initializing Meilisearch data...")
		if err := handler.InitializeData(logger); err != nil {
			return nil, err
		}
	}

	return handler, nil
}

func (m *MeiliSearchHandler) HandleMessage(data []byte, logger *log.Logger) error {
	err := m.ProcessWalData(data, logger)
	if err != nil {
		logger.Printf("Error processing WAL data: %v", err)
		return err
	}
	return nil
}

func (m *MeiliSearchHandler) InitializeData(l *log.Logger) error {
	if !m.EnableInitData {
		l.Println("Data initialization for Meilisearch is disabled")
		return nil
	}

	l.Println("Data initialization for Meilisearch is enable", m.DB)

	return InitializeMeilisearchData(m.DB, m, l)
}
