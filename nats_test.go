package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"nats-jetstream/nat"
	"nats-jetstream/postgres"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
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

func TestMeiliSearchInitialization(t *testing.T) {
	mockHandler := &MockMeiliSearchHandler{}
	logger := zap.NewExample()

	mockHandler.On("InitializeData", logger).Return(nil)

	err := mockHandler.InitializeData(logger)
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}

func TestJetStreamSubscription(t *testing.T) {
	mockSubManager := &MockSubscriptionManager{}
	mockHandler := &MockMeiliSearchHandler{}
	logger := zap.NewExample()

	subject := "TEST_SUBJECT"
	durableName := "TEST_DURABLE"

	mockSubManager.On("SubscribeAsyncWithHandler", subject, durableName, mockHandler, logger).Return(nil)

	err := mockSubManager.SubscribeAsyncWithHandler(subject, durableName, mockHandler, logger)
	assert.NoError(t, err)
	mockSubManager.AssertExpectations(t)
}

func TestPostgresConnection(t *testing.T) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?application_name=pylon&sslmode=disable", User, Password, Host, Port, Database)
	db, err := sql.Open("pgx", dsn)
	if assert.NoError(t, err) && assert.NotNil(t, db) {
		t.Log("TestPostgresConnection passed")
	} else {
		t.Error("TestPostgresConnection failed")
	}
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Ping()
	if assert.NoError(t, err) {
		t.Log("TestPostgresConnection ping passed")
	} else {
		t.Error("TestPostgresConnection ping failed")
	}
	defer db.Close()
}

func TestReplicationDatabase(t *testing.T) {
	ctx := context.Background()

	subject := "TEST_SUBJECT"
	meiliTable := "main.tenants"
	logger := log.New(os.Stdout, "postgres: ", log.LstdFlags)
	connector := &nat.URLConnector{URL: "localhost:4222"}

	_, js, err := connector.Connect(true)
	if assert.NoError(t, err) {
		t.Log("TestReplicationDatabase connection passed")
	} else {
		t.Error("TestReplicationDatabase connection failed")
	}

	go postgres.StartReplicationDatabase(ctx, js.(*nat.JetStreamContextImpl).JS, subject, meiliTable, logger)
}

func TestMeiliSearchHandler(t *testing.T) {
	mockHandler := &MockMeiliSearchHandler{}
	ctx := context.Background()

	msg := []byte(`{"id":1, "name":"Test"}`)

	mockHandler.On("HandleMessage", ctx, msg).Return(nil)

	err := mockHandler.HandleMessage(ctx, msg)
	if assert.NoError(t, err) {
		t.Log("TestMeiliSearchHandler passed")
	} else {
		t.Error("TestMeiliSearchHandler failed")
	}
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
}
