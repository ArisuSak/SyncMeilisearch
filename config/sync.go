package config

import (
	"fmt"
	"log"
	"nats-jetstream/pkg/meilisearch"

	meili "github.com/meilisearch/meilisearch-go"
)

type Manager struct {
    config       *ApplicationConfig
    database     *DatabaseStruct
    handlers     []*meilisearch.MeiliSearchHandler
    walRouter    *Router
    logger       *log.Logger
}

func NewManager(cfg *ApplicationConfig, db *DatabaseStruct, logger *log.Logger) *Manager {
    return &Manager{
        config:   cfg,
        database: db,
        logger:   logger,
    }
}

func (m *Manager) Initialize() error {
    if err := m.setupMeiliSearchHandlers(); err != nil {
        return fmt.Errorf("failed to setup MeiliSearch handlers: %w", err)
    }
    
    if err := m.initializeHandlers(); err != nil {
        return fmt.Errorf("failed to initialize handlers: %w", err)
    }
    
    m.setupWALRouter()
    
    return nil
}

func (m *Manager) setupMeiliSearchHandlers() error {
    client := meili.New(m.config.MeiliSearch.ApiUrl, meili.WithAPIKey(m.config.MeiliSearch.ApiKey))
    // // db, err := sql.Open("pgx", database)
	// if err != nil {
	// 	return fmt.Errorf("failed to open PostgreSQL with DSN: %w", err)
	// }
    for _, syncCfg := range m.config.Sync {
        handler := &meilisearch.MeiliSearchHandler{
            Client:         client,
            BaseURL:        m.config.MeiliSearch.ApiUrl,
            ApiKey:         m.config.MeiliSearch.ApiKey,
            TableName:      syncCfg.Table,
            Index:          syncCfg.Index,
            PK:             syncCfg.PK,
            DB:             m.database.DB,
            EnableInitData: m.config.Initialize,
        }
        m.handlers = append(m.handlers, handler)
    }
    
    return nil
}

func (m *Manager) initializeHandlers() error {
    for _, handler := range m.handlers {
        if err := handler.InitializeData(m.logger); err != nil {
            return fmt.Errorf("failed to initialize handler for table %s: %w", handler.TableName, err)
        }
    }
    return nil
}

func (m *Manager) setupWALRouter() {
    m.walRouter = NewRouter(m.handlers, m.logger)
}

func (m *Manager) GetTableNames() []string {
    var tableNames []string
    for _, syncCfg := range m.config.Sync {
        tableNames = append(tableNames, syncCfg.Table)
    }
    return tableNames
}

func (m *Manager) GetWALCallback() func([]byte) {
    return m.walRouter.GetCallback()
}

func (m *Manager) GetHandlers() []*meilisearch.MeiliSearchHandler {
    return m.handlers
}