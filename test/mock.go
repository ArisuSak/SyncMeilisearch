package test

import (
	"context"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

const (
	Host     = "localhost"
	Port     = 5437
	Database = "pylon"
	User     = "postgres"
	Password = "sonoftruth"
)

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
