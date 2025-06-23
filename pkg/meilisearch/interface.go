package meilisearch

import (
	"database/sql"
	"log"

	meili "github.com/meilisearch/meilisearch-go"
	"go.uber.org/zap"
)

type WalProcessor interface {
	ProcessWalData(data []byte, l *log.Logger) error
}

type DatabaseInitializer interface {
	InitializeData(db *sql.DB, l *zap.Logger) error
}

type MeiliSearchHandler struct {
	Client         meili.ServiceManager
	BaseURL        string
	ApiKey         string
	TableName      string
	Index          string
	PK 		       string 
	EnableInitData bool
	DB             *sql.DB
}

func NewMeiliSearchHandler(db *sql.DB, client  meili.ServiceManager, baseURL, apiKey, tableName, index string, pk string, enableInitData bool, logger *log.Logger) (*MeiliSearchHandler, error) {
	handler := &MeiliSearchHandler{
		Client:         client,
		BaseURL:        baseURL,
		ApiKey:         apiKey,
		TableName:      tableName,
		Index:          index,
		PK:			    pk,
		EnableInitData: enableInitData,
		DB:             db,
	}
	logger.Println("Asssinitializing Meilisearch data...")
	//Check and Create the Index
	if err := handler.CreateIndex(client, logger, index, pk); 
	err != nil {
		logger.Printf("failed to create Meilisearch index: %v", err)
		return nil, err
	}

	if enableInitData {
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

	return InitializeMeilisearchDataByClient(m.DB, m, m.Client,l, m.Index, m.PK)
}
