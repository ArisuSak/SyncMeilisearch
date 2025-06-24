package config

import (
	"encoding/json"
	"fmt"
	"log"

	"nats-jetstream/pkg/meilisearch"
)

type Router struct {
    callbackMap map[string]func([]byte)
    logger      *log.Logger
}

type WALMessage struct {
    Change []struct {
        Table string `json:"table"`
    } `json:"change"`
}

func NewRouter(handlers []*meilisearch.MeiliSearchHandler, logger *log.Logger) *Router {
    callbackMap := make(map[string]func([]byte))
    
    for _, handler := range handlers {
        callbackMap[handler.TableName] = handler.CreateWALCallback(logger)
    }
    
    return &Router{
        callbackMap: callbackMap,
        logger:      logger,
    }
}

func (r *Router) HandleWALData(data []byte) {
    tableName, err := r.parseTableName(data)
    if err != nil {
        r.logger.Printf("Failed to parse WAL message: %v", err)
        return
    }
    
    if callback, exists := r.callbackMap[tableName]; exists {
        r.logger.Printf("Routing WAL message to handler for table: %s", tableName)
        callback(data)
    } else {
        r.logger.Printf("No handler found for table: %s", tableName)
    }
}

func (r *Router) parseTableName(data []byte) (string, error) {
    var walMessage WALMessage
    
    if err := json.Unmarshal(data, &walMessage); err != nil {
        return "", err
    }
    
    if len(walMessage.Change) == 0 {
        return "", fmt.Errorf("no changes found in WAL message")
    }
    
    return walMessage.Change[0].Table, nil
}

func (r *Router) GetCallback() func([]byte) {
    return r.HandleWALData
}