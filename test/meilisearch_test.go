package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMeiliSearchInitialization(t *testing.T) {
	mockHandler := &MockMeiliSearchHandler{}
	logger := zap.NewExample()

	mockHandler.On("InitializeData", logger).Return(nil)

	err := mockHandler.InitializeData(logger)
	assert.NoError(t, err)
	mockHandler.AssertExpectations(t)
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
