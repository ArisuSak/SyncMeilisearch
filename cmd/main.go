package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"nats-jetstream/pkg/meilisearch"
	"nats-jetstream/pkg/nat"
	"nats-jetstream/pkg/postgres"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	meili "github.com/meilisearch/meilisearch-go"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)


type ApplicationConfig struct {
    Initialize   bool          `yaml:"initialize"`
    Database     DatabaseConfig `yaml:"database"`
    MeiliSearch  MeiliSearchConfig `yaml:"meilisearch"`
    Sync         []SyncConfig  `yaml:"sync"`
}

type DatabaseConfig struct {
    Host     string `yaml:"host"`
    Port     string `yaml:"port"`
    Name     string `yaml:"name"`
    User     string `yaml:"user"`
    Password string `yaml:"password"`
}

type Database interface {
    DSN() string
}

type MeiliSearchConfig struct {
    ApiUrl   string `yaml:"api_url"`
    Port   string `yaml:"port"`
    ApiKey string `yaml:"api_key"`
}

type MeiliSearch interface {
    URL() string
    Key() string
}


type SyncConfig struct {
    Table string `yaml:"table"`
    Index string `yaml:"index"`
    PK    string `yaml:"pk,omitempty"`
}

var (
    StreamService string

	Subject      string
	StreamName   string
	ConsumerName string
	DurableName  string
	Url          string

    app *ApplicationConfig
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

    var config ApplicationConfig
    err = yaml.Unmarshal(data, &config)
    if err != nil {
        log.Fatalf("Failed to parse YAML file: %v", err)
    }

    app = &config
}

func main() {
	ctx := context.Background()
	godotenv.Load()
	logger := log.New(os.Stdout, "SyncMeilisearch: ", log.LstdFlags)

	store := postgres.New(ctx, logger)

	if store == nil {
		logger.Fatal("Failed to create store")
	}

	log.Printf("Connection to PostgreSQL successfully")

    fmt.Println("config",app)

	dsn := store.DSN
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		logger.Fatal("Failed to open PostgreSQL with DSN", zap.Error(err))
	}

    client := meili.New(app.MeiliSearch.ApiUrl, meili.WithAPIKey(app.MeiliSearch.ApiKey))

    var meiliHandlers []*meilisearch.MeiliSearchHandler

    for _, syncCfg := range app.Sync {
        handler := &meilisearch.MeiliSearchHandler{
            Client:         client,
            BaseURL:        app.MeiliSearch.ApiUrl,
            ApiKey:         app.MeiliSearch.ApiKey,
            TableName:      syncCfg.Table,
            Index:          syncCfg.Index,
            PK:             syncCfg.PK,
            DB:             db,
            EnableInitData: app.Initialize,
        }
        meiliHandlers = append(meiliHandlers, handler)
    }

    for _, handler := range meiliHandlers {
        if err := handler.InitializeData(logger); err != nil {
            logger.Fatal("Failed to initialize Meilisearch data", zap.Error(err))
        }
    }

    var tableNames []string
    for _, s := range app.Sync {
        tableNames = append(tableNames, s.Table)
    }

    if (StreamService == "jetstream") {
        connector := &nat.URLConnector{URL: Url}

        nc, js, err := connector.Connect(true)

        if err != nil {
            logger.Fatal("Failed to connect to NATS JetStream:", err)
        }

        go postgres.StartReplicationDatabase(ctx, js.(*nat.JetStreamContextImpl).JS, Subject, tableNames, logger)

	    subManager := &nat.SubscriptionManagerImpl{JetStream: js}

        for _, handler := range meiliHandlers {
            err := subManager.SubscribeAsyncWithHandler(Subject, DurableName, handler, logger)
            if err != nil {
                logger.Fatal("Failed to subscribe with handler:", err)
            }
        }

        if err != nil {
            logger.Fatal("Failed to subcribe with Meilisearch handler:", err)
        }

        defer nc.Close()
    } else {

        go postgres.StartReplicationDatabase(ctx, nil, "", tableNames, logger)
    }

	<-ctx.Done()

	logger.Println("Shutting down application...")
}
