package main

import (
	"context"
	"log"
	"os"

	"nats-jetstream/config"

	_ "github.com/jackc/pgx/v5/stdlib"
)




func main() {
    ctx := context.Background()
    logger := log.New(os.Stdout, "SyncMeilisearch: ", log.LstdFlags)
    
    // Load configuration
   cfg, err := config.Load()
    if err != nil {
        log.Fatal("Failed to load configuration:", err)
    }
    
    // Setup database
    db, err := config.NewDatabase(ctx, logger)
    if err != nil {
        logger.Fatal("Database setup failed:", err)
    }
    defer db.Close()
    
    // Setup sync manager
    syncManager := config.NewManager(cfg, db, logger)
    if err := syncManager.Initialize(); err != nil {
        logger.Fatal("Sync manager initialization failed:", err)
    }
    
    // Setup streaming service
    streamingService := config.NewService(cfg, logger)
    
    // Start replication
    if err := streamingService.StartReplication(
        ctx,
        syncManager.GetWALCallback(),
        syncManager.GetTableNames(),
        syncManager.GetHandlers(),
    ); err != nil {
        logger.Fatal("Failed to start replication:", err)
    }
    
    logger.Println("Application started successfully")
    
    <-ctx.Done()
    logger.Println("Shutting down application...")
}
