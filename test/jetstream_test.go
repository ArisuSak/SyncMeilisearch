package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

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
