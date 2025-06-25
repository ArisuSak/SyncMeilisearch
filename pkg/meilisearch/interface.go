package meilisearch

import (
	"database/sql"
	"encoding/json"
	"log"

	meili "github.com/meilisearch/meilisearch-go"
	"go.uber.org/zap"
)

type WalProcessor interface {
	ProcessWalData(data []byte, pk string, l *log.Logger) error
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
	// WalDataChan chan []byte
	DB             *sql.DB
}

// func NewMeiliSearchHandler(db *sql.DB, client  meili.ServiceManager, baseURL, apiKey, tableName, index string, pk string, enableInitData bool, walDataChan chan[]byte, logger *log.Logger) (*MeiliSearchHandler, error) {
// 	handler := &MeiliSearchHandler{
// 		Client:         client,
// 		BaseURL:        baseURL,
// 		ApiKey:         apiKey,
// 		TableName:      tableName,
// 		Index:          index,
// 		PK:			    pk,
// 		EnableInitData: enableInitData,
// 		WalDataChan:    walDataChan,
// 		DB:             db,
// 	}
// 	logger.Println("handler created for Meilisearch with index:", index, "and primary key:", pk)
// 	//Check and Create the Index
// 	if err := handler.CreateIndex(client, logger, index, pk); 
// 	err != nil {
// 		logger.Printf("failed to create Meilisearch index: %v", err)
// 		return nil, err
// 	}

// 	if enableInitData {
// 		if err := handler.InitializeData(logger); err != nil {
// 			return nil, err
// 		}
// 	}

// 	// go func() {
//     //     for data := range walDataChan {
//     //         logger.Printf("Received data in handler: %s", string(data)) // assuming data is []byte
//     //     }
//     // }()

// 	// if walDataChan != nil {
// 	// 	logger.Println("WAL data channel is enabled, starting to listen for changes...")
// 	// 	go func() {
// 	// 		for data := range walDataChan {
// 	// 			if err := handler.HandleMessage(data, logger); err != nil {
// 	// 				logger.Printf("Error handling WAL message: %v", err)
// 	// 			}
// 	// 		}
// 	// 	}()
// 	// }

// 	return handler, nil
// }

func (m *MeiliSearchHandler) HandleMessage(data []byte, logger *log.Logger) error {

	err := m.ProcessWalData(data, logger)
	
	if err != nil {
		logger.Printf("Error processing WAL data: %v", err)
		return err
	}
	return nil
}

func (m *MeiliSearchHandler) InitializeData(l *log.Logger) error {

	err  := m.CreateIndex(m.Client, l, m.Index, m.PK)

	if err != nil {
		l.Printf("Failed to create Meilisearch index: %v", err)
	}

	if !m.EnableInitData {
		l.Println("Data initialization for Meilisearch is disabled")
		return nil
	}

	return InitializeMeilisearchDataByClient(m.DB, m, m.Client,l, m.Index, m.PK)
}

func (m *MeiliSearchHandler) CreateWALCallback(l *log.Logger) func([]byte) {
    return func(data []byte) {
        l.Printf("Received WAL data: %s", string(data))
        
        if !m.isForMyTable(data, l) {
            return 
        }
        
        // l.Printf("Processing WAL data for table: %s", m.TableName)
        if err := m.HandleMessage(data, l); err != nil {
            l.Printf("Failed to handle Meilisearch message for table %s: %v", m.TableName, err)
        }
    }
}

// Helper method to check if WAL message is for this handler's table
func (m *MeiliSearchHandler) isForMyTable(data []byte, l *log.Logger) bool {
    var walMessage struct {
        Change []struct {
            Table string `json:"table"`
        } `json:"change"`
    }
    
    if err := json.Unmarshal(data, &walMessage); err != nil {
        l.Printf("Failed to parse WAL JSON: %v", err)
        return false
    }
    
    if len(walMessage.Change) == 0 {
        return false
    }
    
    // Check if any change in the WAL message is for this table
    for _, change := range walMessage.Change {
        if change.Table == m.TableName {
            return true
        }
    }
    
    return false
}

