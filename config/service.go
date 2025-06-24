package config

import (
	"context"
	"fmt"
	"log"

	"nats-jetstream/pkg/meilisearch"
	"nats-jetstream/pkg/nat"
	"nats-jetstream/pkg/postgres"
)

type Service struct {
    config     *ApplicationConfig
    logger     *log.Logger
}

func NewService(cfg *ApplicationConfig, logger *log.Logger) *Service {
    return &Service{
        config: cfg,
        logger: logger,
    }
}

func (s *Service) StartReplication(ctx context.Context, walCallback func([]byte), tableNames []string, handlers []*meilisearch.MeiliSearchHandler) error {
    if StreamService == "jetstream" {
        return s.startJetStreamReplication(ctx, walCallback, tableNames, handlers)
    }
    
    return s.startDirectReplication(ctx, walCallback, tableNames)
}

func (s *Service) startJetStreamReplication(ctx context.Context, walCallback func([]byte), tableNames []string, handlers []*meilisearch.MeiliSearchHandler) error {
    connector := &nat.URLConnector{URL: Url}
    
    nc, js, err := connector.Connect(true)
    if err != nil {
        return fmt.Errorf("failed to connect to NATS JetStream: %w", err)
    }
    defer nc.Close()
    
    // Start WAL replication
    go postgres.StartReplicationDatabase(ctx, js.(*nat.JetStreamContextImpl).JS, Subject, walCallback, tableNames, s.logger)
    
    // Setup subscriptions
    subManager := &nat.SubscriptionManagerImpl{JetStream: js}
    
    for _, handler := range handlers {
        if err := subManager.SubscribeAsyncWithHandler(Subject, DurableName, handler, s.logger); err != nil {
            return fmt.Errorf("failed to subscribe with handler: %w", err)
        }
    }
    
    return nil
}

func (s *Service) startDirectReplication(ctx context.Context, walCallback func([]byte), tableNames []string) error {
    go postgres.StartReplicationDatabase(ctx, nil, "", walCallback, tableNames, s.logger)
    return nil
}