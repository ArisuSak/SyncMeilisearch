package main

import (
	"context"
	"log"
	"os"

	"nats-jetstream/config"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"

	"gopkg.in/yaml.v3"
)


var (
    StreamService string

	Subject      string
	StreamName   string
	ConsumerName string
	DurableName  string
	Url          string

    app *config.ApplicationConfig
)


func init() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }

    StreamService = os.Getenv("STREAMING_SERVICE")
    Subject = os.Getenv("SUBJECT")
    StreamName = os.Getenv("STREAM_NAME")
    ConsumerName = os.Getenv("CONSUMER_NAME")
    DurableName = os.Getenv("DURABLE_NAME")
    Url = os.Getenv("NATS_URL")

    data, err := os.ReadFile("config.yaml")
    if err != nil {
        log.Fatalf("Failed to read YAML file: %v", err)
    }

    var config config.ApplicationConfig
    err = yaml.Unmarshal(data, &config)
    if err != nil {
        log.Fatalf("Failed to parse YAML file: %v", err)
    }

    app = &config
}

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
