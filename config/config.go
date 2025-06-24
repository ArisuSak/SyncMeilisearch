package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
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


func Load() (*ApplicationConfig, error) {
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
	return app, nil
}