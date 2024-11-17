package main

import (
	"context"
	"log"
	"os"

	"nats-jetstream/meilisearch"
	"nats-jetstream/nat"
	"nats-jetstream/postgres"
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

// TODO: change log to zap.log all
func main() {
	ctx := context.Background()

	logger := log.New(os.Stdout, "postgres: ", log.LstdFlags)

	store := postgres.New(ctx, logger)

	if store == nil {
		logger.Fatal("Failed to create store")
	}

	log.Printf("Connection to PostgreSQL successfully")

	connector := &nat.URLConnector{URL: Url}

	nc, js, err := connector.Connect(true)

	if err != nil {
		logger.Fatal("Failed to set up JetStream")
	}
	defer nc.Close()

	go postgres.StartReplicationDatabase(ctx, js.(*nat.JetStreamContextImpl).JS, Subject, logger)
	subManager := &nat.SubscriptionManagerImpl{JetStream: js}

	meiliHandler := &meilisearch.MeiliSearchHandler{
		BaseURL:   MeiliBaseURL,
		ApiKey:    MeiliApiKey,
		TableName: MeiliTable,
		Index:     MeiliIndex,
	}

	_, err = subManager.SubscribeAsyncWithHandler(Subject, DurableName, meiliHandler, logger)
	if err != nil {
		logger.Fatal("Failed to subcribe with Meilisearch handler:", err)
	}

	<-ctx.Done()
	logger.Println("Shutting down application...")
}
