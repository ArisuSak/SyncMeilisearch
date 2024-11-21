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
)

const (
	Subject      = "TEST_SUBJECT"
	StreamName   = "TEST_STREAM"
	ConsumerName = "TEST_CONSUMER"
	DurableName  = "TEST_DURABLE"
	Url          = "localhost:4222"
)

const (
	MeiliBaseURL = "http://localhost:7700"
	MeiliApiKey  = "my-key"
	MeiliTable   = "main.tenants"
	MeiliIndex   = "tenant"
)

func main() {
	ctx := context.Background()

	logger := log.New(os.Stdout, "postgres: ", log.LstdFlags)

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
	connector := &nat.URLConnector{URL: Url}

	nc, js, err := connector.Connect(true)

	if err != nil {
		logger.Fatal("Failed to set up JetStream")
	}
	defer nc.Close()

	go postgres.StartReplicationDatabase(ctx, js.(*nat.JetStreamContextImpl).JS, Subject, MeiliTable, logger)
	subManager := &nat.SubscriptionManagerImpl{JetStream: js}

	meiliHandler := &meilisearch.MeiliSearchHandler{
		BaseURL:        MeiliBaseURL,
		ApiKey:         MeiliApiKey,
		TableName:      MeiliTable,
		Index:          MeiliIndex,
		DB:             db,
		EnableInitData: true,
	}

	if err := meiliHandler.InitializeData(logger); err != nil {
		logger.Fatal("Failed to initialize Meilisearch data", zap.Error(err))
	}

	err = subManager.SubscribeAsyncWithHandler(Subject, DurableName, meiliHandler, logger)
	if err != nil {
		logger.Fatal("Failed to subcribe with Meilisearch handler:", err)
	}

	<-ctx.Done()
	logger.Println("Shutting down application...")
}
