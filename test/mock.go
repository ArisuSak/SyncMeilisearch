package test

import (
	"context"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

var (
	Host     string
	Port     string
	Database string
	User     string
	Password string
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	Host = os.Getenv("DB_HOST")
	Port = os.Getenv("DB_PORT")
	Database = os.Getenv("DB_NAME")
	User = os.Getenv("DB_USER")
	Password = os.Getenv("DB_PASSWORD")

}

type MockJetStreamContext struct {
	mock.Mock
}

func (m *MockJetStreamContext) PublishAsync(subject string, msg []byte) error {
	args := m.Called(subject, msg)
	return args.Error(0)
}

type MockMeiliSearchHandler struct {
	mock.Mock
}

func (m *MockMeiliSearchHandler) InitializeData(logger *zap.Logger) error {
	args := m.Called(logger)
	return args.Error(0)
}

func (m *MockMeiliSearchHandler) HandleMessage(ctx context.Context, msg []byte) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

type MockSubscriptionManager struct {
	mock.Mock
}

func (m *MockSubscriptionManager) SubscribeAsyncWithHandler(subject, durable string, handler any, logger *zap.Logger) error {
	args := m.Called(subject, durable, handler, logger)
	return args.Error(0)
}
