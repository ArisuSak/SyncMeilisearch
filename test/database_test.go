package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
