package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	"nats-jetstream/pkg/meilisearch"
	"nats-jetstream/pkg/nat"
	"nats-jetstream/pkg/postgres"

	"go.uber.org/zap"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

var (

    StreamService string

	Subject      string
	StreamName   string
	ConsumerName string
	DurableName  string
	Url          string

	MeiliBaseURL string
	MeiliApiKey  string
	MeiliTable   string
	MeiliIndex   string
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

	MeiliBaseURL = os.Getenv("MEILI_BASE_URL")
	MeiliApiKey = os.Getenv("MEILI_API_KEY")
	MeiliTable = os.Getenv("MEILI_TABLE")
	MeiliIndex = os.Getenv("MEILI_INDEX")
}


func main() {
	ctx := context.Background()
	godotenv.Load()
	logger := log.New(os.Stdout, "jet stream: ", log.LstdFlags)

	store := postgres.New(ctx, logger)

	if store == nil {
		logger.Fatal("Failed to create store")
	}

	log.Printf("Connection to PostgreSQL successfully")

	dsn := store.DSN
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		logger.Fatal("Failed to open PostgreSQL with DSN", zap.Error(err))
	}

	meiliHandler := &meilisearch.MeiliSearchHandler{
		BaseURL:        MeiliBaseURL,
		ApiKey:         MeiliApiKey,
		TableName:      MeiliTable,
		Index:          MeiliIndex,
		DB:             db,
		EnableInitData: false,
	}

	if err := meiliHandler.InitializeData(logger); err != nil {
		logger.Fatal("Failed to initialize Meilisearch data", zap.Error(err))
	}

    if (StreamService == "jetstream") {
        connector := &nat.URLConnector{URL: Url}

        nc, js, err := connector.Connect(true)

        if err != nil {
            logger.Fatal("Failed to connect to NATS JetStream:", err)
        }

        go postgres.StartReplicationDatabase(ctx, js.(*nat.JetStreamContextImpl).JS, Subject, MeiliTable, logger)

	    subManager := &nat.SubscriptionManagerImpl{JetStream: js}

        err = subManager.SubscribeAsyncWithHandler(Subject, DurableName, meiliHandler, logger)

        if err != nil {
            logger.Fatal("Failed to subcribe with Meilisearch handler:", err)
        }

        defer nc.Close()
    } else {
        go postgres.StartReplicationDatabase(ctx, nil, "", MeiliTable, logger)
    }

	<-ctx.Done()

	logger.Println("Shutting down application...")
}
