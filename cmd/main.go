package main

import (
	"context"
	"log"
	"os"

	"nats-jetstream/meilisearch"
	"nats-jetstream/nat"
	"nats-jetstream/postgres"

	"github.com/nats-io/nats.go"
)

const (
    Subject = "TEST_SUBJECT"
    StreamName = "TEST_STREAM"
    ConsumerName = "TEST_CONSUMER"
    DurableName = "TEST_DURABLE"
)

//TODO: change log to zap.log all
func main() {
    ctx := context.Background()

    logger := log.New(os.Stdout, "postgres: ", log.LstdFlags)

    store := postgres.New(ctx, logger)

    if store == nil {
        logger.Fatal("Failed to create store")
    }
    
    log.Printf("Connection to PostgreSQL successfully")

    _, js, err := nat.SetupNATS(ctx, StreamName, Subject, logger)

    if err != nil {
        logger.Fatal("Failed to set up JetStream")
    }
    
    go postgres.StartReplicationDatabase(ctx, js, Subject, logger)

    _, err = js.Subscribe(Subject, func(msg *nats.Msg) {
        logger.Printf("Received message: %s", string(msg.Data))

        err := meilisearch.SendToMeilisearch(msg.Data, logger)
        if err != nil {
            logger.Printf("Error sending to Meilisearch: %v", err)     
        }

        msg.Ack() 
    }, nats.Durable(DurableName), nats.ManualAck())

    if err != nil {
        logger.Fatal("Failed to subscribe to subject", err)
    }

    <-ctx.Done()
    logger.Println("Shutting down application...")
}
